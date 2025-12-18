import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from lxml import etree as ET  # pyright: ignore
from pyteomics import mzid

from usigrabber.db.schema import IndexType, MzidFile
from usigrabber.file_parser.helpers import (
    create_search_mod_log_str,
    extract_index_type_and_number,
    extract_score_values,
    extract_unimod_id_or_name,
    extract_xml_subtree,
    get_spectrum_id_format,
    parse_modification_location,
)
from usigrabber.file_parser.models import (
    ModificationDict,
    ModifiedPeptideDict,
    ModifiedPeptideModificationJunctionDict,
    PeptideEvidenceDict,
    PeptideSpectrumMatchDict,
    PSMPeptideEvidenceDict,
    SearchModificationDict,
)
from usigrabber.file_parser.uuid_helpers import (
    generate_deterministic_modification_uuid,
    generate_deterministic_peptide_uuid,
)
from usigrabber.utils import lookup_unimod_id_by_name
from usigrabber.utils.file import parse_basename

logger = logging.getLogger(__name__)


def parse_spectra_data(mzid_path: Path) -> dict[str, tuple[str, IndexType | None]]:
    """
    Parse SpectraData information to extract MS run name and SpectrumIDFormat.
    This approach parses the `<Inputs>` subtree from the mzID file directly,
    to avoid having to go through the entire file.

    Args:
        mzid_path: Path to the mzIdentML file
    Returns:
        Dictionary mapping spectra_id to tuple of (ms_run_name, IndexType enum value or None)
    """
    spectra_data_map: dict[str, tuple[str, IndexType | None]] = {}

    xml_snippet = extract_xml_subtree(mzid_path, tag="Inputs")
    root = ET.fromstring(xml_snippet)

    for child in root:
        if child.tag != "SpectraData":
            continue

        spectra_id = child.get("id")
        if not spectra_id:
            continue
        ms_run_name = child.get("name") or child.get("location")
        if not ms_run_name:
            logger.warning(
                "SpectraData element with id '%s' has no 'name' or 'location' attribute. File: %s",
                spectra_id,
                mzid_path.name,
            )
            continue
        basename = parse_basename(ms_run_name)
        ms_run_name, ext = os.path.splitext(basename)

        # keep stripping extensions until none left
        while ext != "":
            ms_run_name, ext = os.path.splitext(ms_run_name)

        # Extract SpectrumIDFormat accession from nested cvParam
        spectrum_id_format: str | None = None
        spectrum_id_format_cv = child.find(".//SpectrumIDFormat/cvParam")
        if spectrum_id_format_cv is not None:
            spectrum_id_format = spectrum_id_format_cv.get("accession")

        if spectrum_id_format is None:
            logger.warning(
                "SpectraData element with id '%s' has no SpectrumIDFormat cvParam. File: %s",
                spectra_id,
                mzid_path.name,
            )
            continue

        spectra_data_map[spectra_id] = (
            ms_run_name,
            get_spectrum_id_format(cv_param=spectrum_id_format),
        )

    return spectra_data_map


def parse_software_info(reader: mzid.MzIdentML) -> tuple[str | None, str | None]:
    """
    Parse analysis software information from AnalysisSoftware elements.

    Extracts the first analysis software name and version.
    Ignores cases with multiple software.

    Args:
            reader: MzIdentML reader instance

    Returns:
            Tuple of (software_name, software_version), both can be None if not found
    """
    for software in reader.iterfind("AnalysisSoftware"):
        software_name: str | None = software.get("name", None)
        software_version: str | None = software.get("version", None)
        logger.debug(f"Parsed software: {software_name} v{software_version}")
        return software_name, software_version

    return None, None


def parse_threshold_info(reader: mzid.MzIdentML) -> tuple[str | None, float | None]:
    """
    Parse threshold information from SpectrumIdentificationProtocol.

    When using retrieve_refs=False, pyteomics flattens the Threshold element
    into a dictionary with threshold names as keys and values as values.
    For example: {'Mascot:SigThreshold': 0.05, 'Mascot:SigThresholdType': 'homology'}

    Args:
                    reader: MzIdentML reader instance

    Returns:
                    Tuple of (threshold_type, threshold_value)
    """
    threshold_type: str | None = None
    threshold_value: float | None = None

    for protocol in reader.iterfind("SpectrumIdentificationProtocol"):
        threshold: dict[str, Any] = protocol.get("Threshold", {})
        if threshold and len(threshold) > 0:
            # Threshold can contain cvParam or userParam with various threshold types
            # In most cases, pyteomics flattens this into a dict with one key-value pair
            # Example: "Mascot:SigThreshold", "pep:FDR threshold", "distinct peptide-level FDRScore"
            cv_param: dict[str, Any] | None = threshold.get("cvParam")
            user_param: dict[str, Any] | None = threshold.get("userParam")

            threshold_value_raw: str | int | float | None = None

            if cv_param is None and user_param is None:
                # This is a flattened structure - get the first key-value pair
                for key, value in threshold.items():
                    threshold_type = key
                    threshold_value_raw = value
                    break
            else:
                # This is a nested structure with cvParam/userParam
                param = cv_param if cv_param else user_param
                if param is None:
                    continue
                threshold_type = param.get("name")
                threshold_value_raw = param.get("value")

            # Convert to float, handling empty strings and non-numeric values
            if threshold_value_raw is not None:
                try:
                    if isinstance(threshold_value_raw, (int, float)):
                        threshold_value = float(threshold_value_raw)
                    elif isinstance(threshold_value_raw, str):
                        threshold_value = (
                            float(threshold_value_raw) if threshold_value_raw != "" else None
                        )
                    else:
                        threshold_value = None
                except (ValueError, TypeError):
                    threshold_value = None

            break

    return threshold_type, threshold_value


def parse_mzid_metadata(
    reader: mzid.MzIdentML,
    mzid_path: Path,
    project_accession: str,
) -> MzidFile:
    """
    Parse mzID file metadata including software and threshold information.

    Args:
        reader: MzIdentML reader instance
        mzid_path: Path to the mzIdentML file
        project_accession: PRIDE project accession

    Returns:
        MzidFile record with parsed metadata
    """
    software_name, software_version = parse_software_info(reader)
    threshold_type, threshold_value = parse_threshold_info(reader)

    mzid_file = MzidFile(
        project_accession=project_accession,
        file_name=mzid_path.name,
        file_path=str(mzid_path.absolute()),  # TODO: Replace with PRIDE file path
        software_name=software_name,
        software_version=software_version,
        threshold_type=threshold_type,
        threshold_value=threshold_value,
        creation_date=datetime.now(),
    )

    logger.debug(f"Created mzID file record (ID: {mzid_file.id})")
    return mzid_file


def parse_db_sequences(reader: mzid.MzIdentML) -> dict[str, str]:
    """
    Parse DBSequence elements to build mapping from sequence IDs to protein accessions.

    Args:
                    reader: MzIdentML reader instance

    Returns:
                    Dictionary mapping DBSequence IDs to protein accessions
    """
    db_sequence_map: dict[str, str] = {}

    for db_seq in reader.iterfind("DBSequence"):
        seq_id: str = db_seq.get("id", "")
        accession: str = db_seq.get("accession", "")
        if seq_id and accession:
            db_sequence_map[seq_id] = accession

    logger.debug(f"Parsed {len(db_sequence_map)} database sequences")
    return db_sequence_map


def parse_peptides_and_modifications(
    reader: mzid.MzIdentML,
) -> tuple[
    dict[str, uuid.UUID],
    list[ModifiedPeptideDict],
    list[ModificationDict],
    list[ModifiedPeptideModificationJunctionDict],
]:
    """
    Parse Peptide elements and their modifications in a single pass.
    Creates ModifiedPeptide and Modification records with deterministic UUID IDs,
    tracking modifications as we go to avoid duplication.

    Args:
        reader: MzIdentML reader instance

    Returns:
        Tuple of:
        - peptide_id_map: Maps mzID peptide IDs to ModifiedPeptide.id (UUID)
        - List of ModifiedPeptide records created
        - List of Modification records created
        - List of ModifiedPeptideModificationJunction records
    """

    peptide_id_map: dict[str, uuid.UUID] = {}
    # Use dict for deduplication - same modified peptide UUID = same record
    peptides_dict: dict[uuid.UUID, ModifiedPeptideDict] = {}

    # Track modifications as we parse peptides
    modifications_dict: dict[uuid.UUID, ModificationDict] = {}
    junction_set: set[tuple[uuid.UUID, uuid.UUID]] = set()  # (modified_peptide_id, mod_id)

    for peptide_elem in reader.iterfind("Peptide"):
        mzid_peptide_id: str = peptide_elem.get("id", "")
        sequence: str = peptide_elem.get("PeptideSequence", "")
        if not sequence:
            sequence = peptide_elem.get("Seq", "")
        if not mzid_peptide_id or not sequence:
            logger.warning(f"Skipping invalid Peptide element: {peptide_elem}")
            continue

        # Extract modifications to build composite ID
        modifications: dict[str, Any] | list[dict[str, Any]] | None = peptide_elem.get(
            "Modification"
        )
        mod_list = []
        if modifications:
            if not isinstance(modifications, list):
                modifications = [modifications]
            mod_list = modifications

        # Step 1: Parse modifications from raw mzID format
        parsed_mods = parse_modification_list(mod_list)

        # Step 2: Generate deterministic UUID from sequence and modifications
        # This ensures identical modified peptides get the same UUID across all files
        modified_peptide_id = generate_deterministic_peptide_uuid(sequence, parsed_mods)

        peptide_dict: ModifiedPeptideDict = {
            "id": modified_peptide_id,
            "peptide_sequence": sequence,
        }
        # Deduplicate: if same modified_peptide_id already exists, skip adding again and skip mods
        if modified_peptide_id not in peptides_dict:
            peptides_dict[modified_peptide_id] = peptide_dict

            for mod in parsed_mods:
                mod_id = mod["id"]
                modifications_dict[mod_id] = mod
                # Add junction entry (set automatically deduplicates)
                junction_set.add((modified_peptide_id, mod_id))

        peptide_id_map[mzid_peptide_id] = modified_peptide_id

    # Convert collections to lists for batch insertion
    peptides_batch = list(peptides_dict.values())
    modifications_batch = list(modifications_dict.values())
    junction_batch: list[ModifiedPeptideModificationJunctionDict] = [
        {"modified_peptide_id": mp_id, "modification_id": mod_id} for mp_id, mod_id in junction_set
    ]

    logger.debug(
        f"Created {len(peptides_batch)} unique modified peptide records, "
        f"{len(modifications_batch)} unique modifications, and "
        f"{len(junction_batch)} unique junction entries"
    )
    return peptide_id_map, peptides_batch, modifications_batch, junction_batch


def parse_modification_list(modifications: list[dict[str, Any]]) -> list[ModificationDict]:
    """
    Parse raw modification data from mzID into standardized format.

    Args:
        modifications: List of raw modification dictionaries from mzID

    Returns:
        List of parsed modification dicts with keys: id, unimod_id, name, location, modified_residue
    """
    if not modifications:
        return []

    parsed_mods: list[ModificationDict] = []
    for mod in modifications:
        # Extract UNIMOD ID, name, location, and residues
        unimod_id, name = extract_unimod_id_or_name(mod)
        location, residues = parse_modification_location(mod)

        mod_uuid = generate_deterministic_modification_uuid(unimod_id, name, location, residues)
        mod_dict: ModificationDict = {
            "id": mod_uuid,
            "unimod_id": unimod_id,
            "name": name,
            "location": location,
            "modified_residue": residues,
        }
        parsed_mods.append(mod_dict)

    return parsed_mods


def parse_peptide_evidence(
    reader: mzid.MzIdentML,
    db_sequence_map: dict[str, str],
) -> tuple[dict[str, uuid.UUID], list[PeptideEvidenceDict]]:
    """
    Parse PeptideEvidence elements.

    Args:
                    reader: MzIdentML reader instance
                    db_sequence_map: Mapping from DBSequence IDs to protein accessions

    Returns:
                    Tuple of:
                    - pe_id_map: Maps mzID peptide evidence IDs to database PeptideEvidence.id
                    - List of PeptideEvidence records created
    """

    pe_id_map: dict[str, uuid.UUID] = {}

    peptide_evidence_batch: list[PeptideEvidenceDict] = []

    for pe_elem in reader.iterfind("PeptideEvidence"):
        mzid_pe_id: str = pe_elem.get("id", "")
        if not mzid_pe_id:
            continue

        db_sequence_ref: str = pe_elem.get("dBSequence_ref", "")

        # Resolve protein accession from DBSequence reference
        protein_accession: str | None = db_sequence_map.get(db_sequence_ref)

        # Create peptide evidence record
        pe_id = uuid.uuid4()
        pe_dict: PeptideEvidenceDict = {
            "id": pe_id,
            "protein_accession": protein_accession,
            "is_decoy": pe_elem.get("isDecoy", None),
            "start_position": pe_elem.get("start", None),
            "end_position": pe_elem.get("end", None),
            "pre_residue": pe_elem.get("pre", None),
            "post_residue": pe_elem.get("post", None),
        }
        peptide_evidence_batch.append(pe_dict)
        pe_id_map[mzid_pe_id] = pe_id

    logger.debug(f"Created {len(peptide_evidence_batch)} peptide evidence records")
    return pe_id_map, peptide_evidence_batch


def parse_psms(
    reader: mzid.MzIdentML,
    project_accession: str,
    mzid_file_id: uuid.UUID,
    peptide_id_map: dict[str, uuid.UUID],
    pe_id_map: dict[str, uuid.UUID],
    spectra_data_map: dict[str, tuple[str, IndexType | None]],
) -> tuple[
    list[PeptideSpectrumMatchDict], list[PSMPeptideEvidenceDict], list[SearchModificationDict]
]:
    """
    Parse SpectrumIdentificationResult elements.

    Args:
                    reader: MzIdentML reader instance
                    project_accession: PRIDE project accession
                    mzid_file_id: Database ID of the MzidFile record
                    peptide_id_map: Mapping from mzID peptide refs to ModifiedPeptide.id (UUID)
                    pe_id_map: Mapping from mzID peptide evidence IDs to database PeptideEvidence.id

    Returns:
                    Tuple of:
                    - List of PeptideSpectrumMatch records
                    - List of PSMPeptideEvidence junction records
                    - List of SearchModification records
    """

    psm_batch: list[PeptideSpectrumMatchDict] = []
    junction_batch: list[PSMPeptideEvidenceDict] = []
    search_mod_batch: list[SearchModificationDict] = []

    list_protocol_map: dict[str, str] = {}
    for si in reader.iterfind("SpectrumIdentification"):
        sil_ref = si.get("spectrumIdentificationList_ref", "")
        sip_ref = si.get("spectrumIdentificationProtocol_ref", "")
        if sil_ref and sip_ref:
            list_protocol_map[sil_ref] = sip_ref

    protocol_search_mods_map: dict[str, list[int]] = {}
    for sip in reader.iterfind("SpectrumIdentificationProtocol"):
        search_mods = sip.get("ModificationParams", {}).get("SearchModification", [])
        mod_names = set()
        for mod in search_mods:
            # Get modification name which is the last key in the dict
            mod_name = list(mod.keys())[-1]
            mod_names.add(mod_name)
        unimod_id_list = [lookup_unimod_id_by_name(mod_name) for mod_name in mod_names]
        protocol_search_mods_map[sip.get("id", "")] = [
            unimod_id for unimod_id in unimod_id_list if unimod_id is not None
        ]
    search_mod_counts = set[int]()
    for mods in protocol_search_mods_map.values():
        search_mod_counts.add(len(mods))

    for sil in reader.iterfind("SpectrumIdentificationList"):
        for sir in sil.get("SpectrumIdentificationResult", []):
            spectrum_id: str = sir.get("spectrumID", "")

            # Extract USI-related fields from the SpectrumIdentificationResult
            ms_run: str | None = None
            index_type, index_number = extract_index_type_and_number(sir)

            # Get list of spectrum identification items (PSMs)
            sii_list: dict[str, Any] | list[dict[str, Any]] = sir.get(
                "SpectrumIdentificationItem", []
            )
            if not isinstance(sii_list, list):
                sii_list = [sii_list]

            for sii in sii_list:
                # Look up modified peptide ID using mzID peptide reference
                peptide_ref: str = sii.get("peptide_ref", "")
                modified_peptide_id: uuid.UUID | None = peptide_id_map.get(peptide_ref)

                if not modified_peptide_id:
                    logger.warning(f"Peptide_ref '{peptide_ref}' not found in map")
                    continue

                # Extract score values using helper function
                score_values: dict[str, float] = extract_score_values(sii)

                spectraData_ref = sir.get("spectraData_ref", "")
                spectra_data = spectra_data_map.get(spectraData_ref)
                if spectra_data:
                    ms_run = spectra_data[0]
                    if spectra_data[1]:
                        index_type = spectra_data[1]

                # Create PSM record
                psm_id = uuid.uuid4()
                psm: PeptideSpectrumMatchDict = {
                    "id": psm_id,
                    "project_accession": project_accession,
                    "mzid_file_id": mzid_file_id,
                    "modified_peptide_id": modified_peptide_id,
                    "spectrum_id": spectrum_id,
                    "charge_state": sii.get("chargeState"),
                    "experimental_mz": sii.get("experimentalMassToCharge"),
                    "calculated_mz": sii.get("calculatedMassToCharge"),
                    "score_values": score_values if score_values else None,
                    "rank": sii.get("rank"),
                    "pass_threshold": sii.get("passThreshold"),
                    "index_type": index_type,
                    "index_number": index_number,
                    "ms_run": ms_run,
                }
                psm_batch.append(psm)

                for unimod_id in protocol_search_mods_map.get(
                    list_protocol_map.get(sil.get("id", ""), ""), []
                ):
                    # create search modification record
                    search_mod: SearchModificationDict = {
                        "id": uuid.uuid4(),
                        "psm_id": psm["id"],
                        "unimod_id": unimod_id,
                    }
                    search_mod_batch.append(search_mod)

                # Link PSM to peptide evidence via junction table
                pe_refs: dict[str, Any] | list[dict[str, Any]] = sii.get("PeptideEvidenceRef", [])
                if not isinstance(pe_refs, list):
                    pe_refs = [pe_refs]
                for pe_ref in pe_refs:
                    pe_ref_id: str = pe_ref.get("peptideEvidence_ref", "")
                    db_pe_id: uuid.UUID | None = pe_id_map.get(pe_ref_id)

                    if db_pe_id:
                        junction_dict: PSMPeptideEvidenceDict = {
                            "id": uuid.uuid4(),
                            "psm_id": psm_id,
                            "peptide_evidence_id": db_pe_id,
                        }
                        junction_batch.append(junction_dict)

    logger.debug(f"Parsed {len(psm_batch)} PSMs and {len(junction_batch)} junctions")
    logger.debug(
        f"Each PSM is linked to {create_search_mod_log_str(search_mod_counts)} search modification(s)"
    )
    return psm_batch, junction_batch, search_mod_batch
