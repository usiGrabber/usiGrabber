import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from pyteomics import mzid

from usigrabber.db.schema import (
    MzidFile,
)
from usigrabber.file_parser.helpers import (
    extract_score_values,
    extract_unimod_id_and_name,
    extract_usi_fields,
    parse_modification_location,
)

logger = logging.getLogger(__name__)


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


def parse_peptides(
    reader: mzid.MzIdentML,
) -> tuple[dict[str, str], dict[str, list[dict[str, Any]]], list[dict]]:
    """
    Parse Peptide elements.
    Creates ModifiedPeptide records with IDs composed of sequence + modifications.

    Args:
                    reader: MzIdentML reader instance

    Returns:
                    Tuple of:
                    - peptide_id_map: Maps mzID peptide IDs to ModifiedPeptide.id (string)
                    - peptide_mods: Maps ModifiedPeptide.id to list of modification data
                    - List of ModifiedPeptide records created
    """

    peptide_id_map: dict[str, str] = {}
    peptide_mods: dict[str, list[dict[str, Any]]] = {}
    # Use dict for deduplication - same modified peptide ID = same record
    peptides_dict: dict[str, dict] = {}

    for peptide_elem in reader.iterfind("Peptide"):
        mzid_peptide_id = peptide_elem.get("id", "")
        sequence = peptide_elem.get("PeptideSequence", "")
        if not mzid_peptide_id or not sequence:
            logger.warning(f"Skipping invalid Peptide element: {peptide_elem}")
            continue

        # Extract modifications to build composite ID
        modifications = peptide_elem.get("Modification")
        mod_list = []
        if modifications:
            if not isinstance(modifications, list):
                modifications = [modifications]
            mod_list = modifications

        # Generate modified peptide ID: sequence + sorted modification signature
        mod_signature = _generate_modification_signature(mod_list)
        modified_peptide_id = f"{sequence}__{mod_signature}" if mod_signature else sequence

        peptide_dict = {
            "id": modified_peptide_id,
            "peptide_sequence": sequence,
        }
        # Deduplicate: if same modified_peptide_id already exists, this overwrites
        peptides_dict[modified_peptide_id] = peptide_dict
        peptide_id_map[mzid_peptide_id] = modified_peptide_id

        if mod_list:
            peptide_mods[modified_peptide_id] = mod_list

    # Convert dict to list for batch insertion
    peptides_batch = list(peptides_dict.values())
    logger.debug(f"Created {len(peptides_batch)} unique modified peptide records")
    return peptide_id_map, peptide_mods, peptides_batch


def _generate_modification_signature(modifications: list[dict[str, Any]]) -> str:
    """
    Generate a deterministic signature string from modifications for ID generation.

    Args:
        modifications: List of modification dictionaries from mzID

    Returns:
        Sorted modification signature string (e.g., "unimod35@5_unimod4@10")
    """
    if not modifications:
        return ""

    mod_parts = []
    for mod in modifications:
        # Extract UNIMOD ID and location
        unimod_id, name = extract_unimod_id_and_name(mod)
        location, residues = parse_modification_location(mod)

        # Use unimod ID if available, otherwise use name
        mod_identifier = f"unimod{unimod_id}" if unimod_id else (name or "unknown")
        # Clean identifier to remove special characters
        mod_identifier = mod_identifier.replace(":", "_").replace(" ", "_")

        loc_str = str(location) if location is not None else "unk"
        mod_parts.append(f"{mod_identifier}@{loc_str}")

    # Sort by location to ensure deterministic IDs
    mod_parts.sort()
    return "_".join(mod_parts)


def parse_peptide_evidence(
    reader: mzid.MzIdentML,
    db_sequence_map: dict[str, str],
) -> tuple[dict[str, uuid.UUID], list[dict]]:
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

    peptide_evidence_batch: list[dict] = []

    for pe_elem in reader.iterfind("PeptideEvidence"):
        mzid_pe_id: str = pe_elem.get("id", "")
        if not mzid_pe_id:
            continue

        db_sequence_ref: str = pe_elem.get("dBSequence_ref", "")

        # Resolve protein accession from DBSequence reference
        protein_accession: str | None = db_sequence_map.get(db_sequence_ref)

        # Create peptide evidence record
        pe_id = uuid.uuid4()
        pe_dict = {
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
    peptide_id_map: dict[str, str],
    pe_id_map: dict[str, uuid.UUID],
) -> tuple[list[dict], list[dict]]:
    """
    Parse SpectrumIdentificationResult elements.

    Args:
                    reader: MzIdentML reader instance
                    project_accession: PRIDE project accession
                    mzid_file_id: Database ID of the MzidFile record
                    peptide_id_map: Mapping from mzID peptide refs to ModifiedPeptide.id (string)
                    pe_id_map: Mapping from mzID peptide evidence IDs to database PeptideEvidence.id

    Returns:
                    Tuple of:
                    - List of PeptideSpectrumMatch records
                    - List of PSMPeptideEvidence junction records
    """

    psm_batch: list[dict] = []
    junction_batch: list[dict] = []

    for sir in reader.iterfind("SpectrumIdentificationResult"):
        spectrum_id: str = sir.get("spectrumID", "")

        # Extract USI-related fields from the SpectrumIdentificationResult
        index_type, index_number, ms_run = extract_usi_fields(sir)

        # Get list of spectrum identification items (PSMs)
        sii_list: dict[str, Any] | list[dict[str, Any]] = sir.get("SpectrumIdentificationItem", [])
        if not isinstance(sii_list, list):
            sii_list = [sii_list]

        for sii in sii_list:
            # Look up modified peptide ID using mzID peptide reference
            peptide_ref: str = sii.get("peptide_ref", "")
            modified_peptide_id: str | None = peptide_id_map.get(peptide_ref)

            if not modified_peptide_id:
                logger.warning(f"Peptide_ref '{peptide_ref}' not found in map")
                continue

            # Extract score values using helper function
            score_values: dict[str, float] = extract_score_values(sii)

            # Create PSM record
            psm_id = uuid.uuid4()
            psm = {
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

            # Link PSM to peptide evidence via junction table
            pe_refs: dict[str, Any] | list[dict[str, Any]] = sii.get("PeptideEvidenceRef", [])
            if not isinstance(pe_refs, list):
                pe_refs = [pe_refs]
            for pe_ref in pe_refs:
                pe_ref_id: str = pe_ref.get("peptideEvidence_ref", "")
                db_pe_id: uuid.UUID | None = pe_id_map.get(pe_ref_id)

                if db_pe_id:
                    junction_batch.append(
                        {
                            "id": uuid.uuid4(),
                            "psm_id": psm_id,
                            "peptide_evidence_id": db_pe_id,
                        }
                    )

    logger.debug(f"Parsed {len(psm_batch)} PSMs and {len(junction_batch)} junctions")
    return psm_batch, junction_batch


# TODO: Create separate modifications per residue if multiple residues modified?
def link_modifications(
    peptide_mods: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict], list[dict]]:
    """
    Create Modification records and junction table entries linking to ModifiedPeptides.

    Args:
        peptide_mods: Mapping from ModifiedPeptide.id to list of modification data

    Returns:
        Tuple of:
        - List of Modification records created
        - List of ModifiedPeptideModificationJunction records
    """

    # Use dict for deduplication - key is (unimod_id, name, position, modified_residue)
    modifications_dict: dict[tuple, dict] = {}
    # Track unique key -> UUID mapping for junction table
    mod_id_map: dict[tuple, uuid.UUID] = {}
    # Use set to deduplicate junction entries
    junction_set: set[tuple[str, uuid.UUID]] = set()

    for modified_peptide_id, mods in peptide_mods.items():
        for mod in mods:
            # Extract UNIMOD ID and name
            unimod_id, name = extract_unimod_id_and_name(mod)

            location, residues = parse_modification_location(mod)

            # Skip modifications without valid location
            if location is None:
                logger.warning(f"No location found for modification: {mod}")
                continue

            # Create unique key for deduplication
            mod_key = (unimod_id, name, location, residues)

            # Only create new modification if we haven't seen this exact one before
            if mod_key not in modifications_dict:
                mod_id = uuid.uuid4()
                modifications_dict[mod_key] = {
                    "id": mod_id,
                    "unimod_id": unimod_id,
                    "name": name,
                    "position": location,
                    "modified_residue": residues,
                }
                mod_id_map[mod_key] = mod_id
            else:
                # Reuse existing modification UUID
                mod_id = mod_id_map[mod_key]

            # Add junction entry (set automatically deduplicates)
            junction_set.add((modified_peptide_id, mod_id))

    # Convert dict and set to lists for batch insertion
    modifications_batch = list(modifications_dict.values())
    junction_batch = [
        {"modified_peptide_id": mp_id, "modification_id": mod_id} for mp_id, mod_id in junction_set
    ]

    logger.debug(
        f"Created {len(modifications_batch)} unique modifications and {len(junction_batch)}"
        " unique junction entries"
    )
    return modifications_batch, junction_batch
