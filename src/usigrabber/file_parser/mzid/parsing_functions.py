"""
mzID Parsing Functions

Individual parsing functions for different sections of mzIdentML files.
Each function is responsible for parsing a specific type of element and
returning the appropriate data structures.
"""

import logging
import os
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any

from pyteomics import mzid

from usigrabber.db.schema import (
    IndexType,
    MzidFile,
    Peptide,
    PeptideEvidence,
    PeptideModification,
    PeptideSpectrumMatch,
    PSMPeptideEvidence,
)
from usigrabber.file_parser.mzid.helpers import (
    extract_index_type_and_number,
    extract_score_values,
    extract_unimod_id_and_name,
    parse_modification_location,
)
from usigrabber.utils.file import parse_basename

logger = logging.getLogger(__name__)

SPECTRUM_ID_FORMAT_MAPPING = {
    "MS:1000774": IndexType.index,
    "MS:1000776": IndexType.scan,
    "MS:1000768": IndexType.scan,
    "MS:1001530": IndexType.nativeId,
}
exceptions = set()


def get_spectrum_id_format(cv_param: str) -> IndexType | None:
    """
    Map SpectrumIDFormat accession to human-readable format.

    Args:
        accession: SpectrumIDFormat accession

    Returns:
        Human-readable format string or None if not found
    """
    id_format = SPECTRUM_ID_FORMAT_MAPPING.get(cv_param)
    if not id_format:
        if cv_param not in exceptions:
            # only print each exception once
            logger.warning("Unknown SpectrumIDFormat accession: %s", cv_param)
            exceptions.add(cv_param)
        return None
    return id_format


def parse_spectra_data(mzid_path: Path) -> dict[str, tuple[str, IndexType | None]]:
    """
    Parse SpectraData information to extract MS run name and SpectrumIDFormat.

    Args:
        mzid_path: Path to the mzIdentML file

    Returns:
        Dictionary mapping spectra_id to tuple of (ms_run_name, spectrum_id_format_accession)
    """
    spectra_data_dict: dict[str, tuple[str, IndexType | None]] = {}

    try:
        tree = ET.parse(mzid_path)
        root = tree.getroot()

        # Define namespace (mzIdentML uses the 1.1 namespace)
        ns = {"mzid": "http://psidev.info/psi/pi/mzIdentML/1.1"}

        # Find all SpectraData elements
        spectra_data_elements = root.findall(".//mzid:SpectraData", ns)

        for spectra_data in spectra_data_elements:
            # Extract the 'id' attribute as key
            spectra_id = spectra_data.get("id")
            if not spectra_id:
                continue

            # Extract the 'name' attribute (used as MS run identifier)
            # If 'name' doesn't exist, use 'location' as fallback
            ms_run_name = spectra_data.get("name") or spectra_data.get("location")
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
            spectrum_id_format_cv = spectra_data.find(".//mzid:SpectrumIDFormat/mzid:cvParam", ns)
            if spectrum_id_format_cv is not None:
                spectrum_id_format = spectrum_id_format_cv.get("accession")

            if spectrum_id_format is None:
                logger.warning(
                    "SpectraData element with id '%s' has no SpectrumIDFormat cvParam. File: %s",
                    spectra_id,
                    mzid_path.name,
                )
                continue

            spectra_data_dict[spectra_id] = (
                ms_run_name,
                get_spectrum_id_format(cv_param=spectrum_id_format),
            )

    except ET.ParseError as e:
        logger.error(f"Failed to parse SpectraData from XML: {e}")

    return spectra_data_dict


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
) -> tuple[dict[str, uuid.UUID], dict[uuid.UUID, list[dict[str, Any]]], list[Peptide]]:
    """
    Parse Peptide elements.
    Creates a new Peptide record for each mzID Peptide element.

    Args:
                    reader: MzIdentML reader instance

    Returns:
                    Tuple of:
                    - peptide_id_map: Maps mzID peptide IDs to database Peptide.id
                    - peptide_mods: Maps database Peptide.id to list of modification data
                    - List of Peptide records created
    """

    peptide_id_map: dict[str, uuid.UUID] = {}
    peptide_mods: dict[uuid.UUID, list[dict[str, Any]]] = {}
    peptides_batch: list[Peptide] = []

    for peptide_elem in reader.iterfind("Peptide"):
        mzid_peptide_id: str = peptide_elem.get("id", "")
        sequence: str = peptide_elem.get("PeptideSequence", "")

        if not mzid_peptide_id or not sequence:
            logger.warning(f"Skipping invalid Peptide element: {peptide_elem}")
            continue

        peptide = Peptide(sequence=sequence, length=len(sequence))
        peptides_batch.append(peptide)

        peptide_id_map[mzid_peptide_id] = peptide.id

        # Store modification data for later processing
        modifications: dict[str, Any] | list[dict[str, Any]] | None = peptide_elem.get(
            "Modification"
        )
        if modifications:
            if not isinstance(modifications, list):
                modifications = [modifications]

            # Store modifications for this peptide
            peptide_mods[peptide.id] = []

            for mod in modifications:
                peptide_mods[peptide.id].append(mod)

    logger.debug(f"Created {len(peptides_batch)} peptide records")
    return peptide_id_map, peptide_mods, peptides_batch


def parse_peptide_evidence(
    reader: mzid.MzIdentML,
    db_sequence_map: dict[str, str],
) -> tuple[dict[str, uuid.UUID], list[PeptideEvidence]]:
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

    peptide_evidence_batch: list[PeptideEvidence] = []

    for pe_elem in reader.iterfind("PeptideEvidence"):
        mzid_pe_id: str = pe_elem.get("id", "")
        db_sequence_ref: str = pe_elem.get("dBSequence_ref", "")

        if not mzid_pe_id:
            continue

        # Resolve protein accession from DBSequence reference
        protein_accession: str | None = db_sequence_map.get(db_sequence_ref)

        # Create peptide evidence record
        peptide_evidence = PeptideEvidence(
            protein_accession=protein_accession,
            is_decoy=pe_elem.get("isDecoy", None),
            start_position=pe_elem.get("start", None),
            end_position=pe_elem.get("end", None),
            pre_residue=pe_elem.get("pre", None),
            post_residue=pe_elem.get("post", None),
        )

        peptide_evidence_batch.append(peptide_evidence)

        pe_id_map[mzid_pe_id] = peptide_evidence.id

    logger.debug(f"Created {len(peptide_evidence_batch)} peptide evidence records")
    return pe_id_map, peptide_evidence_batch


def parse_psms(
    reader: mzid.MzIdentML,
    project_accession: str,
    mzid_file_id: uuid.UUID,
    peptide_id_map: dict[str, uuid.UUID],
    pe_id_map: dict[str, uuid.UUID],
    spectra_data_map: dict[str, tuple[str, IndexType | None]],
) -> tuple[list[PeptideSpectrumMatch], list[PSMPeptideEvidence]]:
    """
    Parse SpectrumIdentificationResult elements.

    Args:
                    reader: MzIdentML reader instance
                    project_accession: PRIDE project accession
                    mzid_file_id: Database ID of the MzidFile record
                    peptide_id_map: Mapping from mzID peptide IDs to database Peptide.id
                    pe_id_map: Mapping from mzID peptide evidence IDs to database PeptideEvidence.id

    Returns:
                    Tuple of:
                    - List of PeptideSpectrumMatch records
                    - List of PSMPeptideEvidence junction records
    """

    psm_batch: list[PeptideSpectrumMatch] = []
    junction_batch: list[PSMPeptideEvidence] = []

    for sir in reader.iterfind("SpectrumIdentificationResult"):
        spectrum_id: str = sir.get("spectrumID", "")

        # Extract USI-related fields from the SpectrumIdentificationResult
        ms_run: str | None = None
        index_type, index_number = extract_index_type_and_number(sir)

        # Get list of spectrum identification items (PSMs)
        sii_list: dict[str, Any] | list[dict[str, Any]] = sir.get("SpectrumIdentificationItem", [])
        if not isinstance(sii_list, list):
            sii_list = [sii_list]

        for sii in sii_list:
            # Look up database peptide ID
            peptide_ref: str = sii.get("peptide_ref", "")
            db_peptide_id: uuid.UUID | None = peptide_id_map.get(peptide_ref)

            if not db_peptide_id:
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
            psm = PeptideSpectrumMatch(
                project_accession=project_accession,
                mzid_file_id=mzid_file_id,
                peptide_id=db_peptide_id,
                spectrum_id=spectrum_id,
                charge_state=sii.get("chargeState", None),
                experimental_mz=sii.get("experimentalMassToCharge", None),
                calculated_mz=sii.get("calculatedMassToCharge", None),
                score_values=score_values if score_values else None,
                rank=sii.get("rank", None),
                pass_threshold=sii.get("passThreshold", None),
                index_type=index_type,
                index_number=index_number,
                ms_run=ms_run,
            )

            psm_batch.append(psm)

            # Link PSM to peptide evidence via junction table
            pe_refs: dict[str, Any] | list[dict[str, Any]] = sii.get("PeptideEvidenceRef", [])
            if not isinstance(pe_refs, list):
                pe_refs = [pe_refs]
            for pe_ref in pe_refs:
                pe_ref_id: str = pe_ref.get("peptideEvidence_ref", "")
                db_pe_id: uuid.UUID | None = pe_id_map.get(pe_ref_id)

                if db_pe_id:
                    junction = PSMPeptideEvidence(
                        psm_id=psm.id,
                        peptide_evidence_id=db_pe_id,
                    )
                    junction_batch.append(junction)

    logger.debug(f"Parsed {len(psm_batch)} PSMs and {len(junction_batch)} junctions")
    return psm_batch, junction_batch


def link_modifications(
    peptide_mods: dict[uuid.UUID, list[dict[str, Any]]],
) -> list[PeptideModification]:
    """
    Create PeptideModification records for all peptides with modifications.

    Args:
        peptide_mods: Mapping from database Peptide.id to list of modification data

    Returns:
        List of PeptideModification records created
    """

    peptide_mod_batch: list[PeptideModification] = []

    for peptide_id, mods in peptide_mods.items():
        for mod in mods:
            # Extract UNIMOD ID
            unimod_id, name = extract_unimod_id_and_name(mod)

            location, residues = parse_modification_location(mod)

            # Skip modifications without valid location
            if location is None:
                logger.warning(f"No location found for modification: {mod}")

            # Create modification record
            peptide_mod = PeptideModification(
                peptide_id=peptide_id,
                unimod_id=unimod_id,
                name=name,
                position=location,
                modified_residue=residues,
            )
            peptide_mod_batch.append(peptide_mod)

    logger.debug(f"Created {len(peptide_mod_batch)} peptide modification records")
    return peptide_mod_batch
