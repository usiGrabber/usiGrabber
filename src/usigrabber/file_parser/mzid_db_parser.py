"""
mzID Database Parser

Parses mzIdentML files using pyteomics and populates the database with:
- Peptides (one per mzID Peptide element, not deduplicated)
- Peptide Modifications (UNIMOD-based)
- Peptide Evidence (protein mappings)
- Peptide Spectrum Matches (PSMs)
- PSM-PeptideEvidence junction records

Uses retrieve_refs=False to avoid handling deduplication in the code.
"""

import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, NamedTuple

from pyteomics import mzid
from pyteomics.auxiliary import PyteomicsError
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session

from usigrabber.db.engine import load_db_engine
from usigrabber.db.schema import (
    MzidFile,
    Peptide,
    PeptideEvidence,
    PeptideModification,
    PeptideSpectrumMatch,
    PSMPeptideEvidence,
)
from usigrabber.file_parser.errors import MzidImportError, MzidParseError
from usigrabber.file_parser.models import ImportStats
from usigrabber.file_parser.mzid_helpers import (
    extract_score_values,
    extract_unimod_id,
    parse_modification_location,
)

logger = logging.getLogger(__name__)

# Public API
__all__ = [
    "ParsedMzidData",
    "parse_mzid_file",
    "import_mzid",
]


class ParsedMzidData(NamedTuple):
    """Container for all parsed data from an mzIdentML file."""

    mzid_file: MzidFile
    peptides: list[Peptide]
    peptide_modifications: list[PeptideModification]
    peptide_evidence: list[PeptideEvidence]
    psms: list[PeptideSpectrumMatch]
    psm_peptide_evidence_junctions: list[PSMPeptideEvidence]


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
        software_name = software.get("name", None)
        software_version = software.get("version", None)
        logger.debug(f"Parsed software: {software_name} v{software_version}")
        return software_name, software_version

    return None, None


def parse_threshold_info(reader: mzid.MzIdentML) -> tuple[str | None, float | None]:
    """
    Parse threshold information from SpectrumIdentificationProtocol.

    Args:
            reader: MzIdentML reader instance

    Returns:
            Tuple of (threshold_type, threshold_value)
    """
    threshold_type = None
    threshold_value = None

    for protocol in reader.iterfind("SpectrumIdentificationProtocol"):
        threshold = protocol.get("Threshold", {})
        if threshold and len(threshold) > 0:
            # Threshold can contain cvParam or userParam with various threshold types
            # Example: "Mascot:SigThreshold", "pep:FDR threshold", "distinct peptide-level FDRScore"
            # Get first key-value pair
            cv_param = threshold.get("cvParam", None)
            user_param = threshold.get("userParam", None)

            param = cv_param if cv_param else user_param
            if param is None:
                continue

            name = param.get("name", None)
            value = param.get("value", None)
            threshold_type = name
            threshold_value_raw = value

            # Convert to float, handling empty strings
            try:
                threshold_value = float(threshold_value_raw) if threshold_value_raw != "" else None
            except (ValueError, TypeError):
                threshold_value = None

            break

    return threshold_type, threshold_value


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
        seq_id = db_seq.get("id", "")
        accession = db_seq.get("accession", "")
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
    peptides_batch = []

    for peptide_elem in reader.iterfind("Peptide"):
        mzid_peptide_id = peptide_elem.get("id", "")
        sequence = peptide_elem.get("PeptideSequence", "")

        if not mzid_peptide_id or not sequence:
            continue

        peptide = Peptide(sequence=sequence, length=len(sequence))
        peptides_batch.append(peptide)

        peptide_id_map[mzid_peptide_id] = peptide.id

        # Store modification data for later processing
        modifications = peptide_elem.get("Modification")
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

    peptide_evidence_batch = []

    for pe_elem in reader.iterfind("PeptideEvidence"):
        mzid_pe_id = pe_elem.get("id", "")
        db_sequence_ref = pe_elem.get("dBSequence_ref", "")

        if not mzid_pe_id:
            continue

        # Resolve protein accession from DBSequence reference
        protein_accession = db_sequence_map.get(db_sequence_ref)

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

    psm_batch = []
    junction_batch = []

    for sir in reader.iterfind("SpectrumIdentificationResult"):
        spectrum_id = sir.get("spectrumID", "")

        # Get list of spectrum identification items (PSMs)
        sii_list = sir.get("SpectrumIdentificationItem", [])
        if not isinstance(sii_list, list):
            sii_list = [sii_list]

        for sii in sii_list:
            # Look up database peptide ID
            peptide_ref = sii.get("peptide_ref", "")
            db_peptide_id = peptide_id_map.get(peptide_ref)

            if not db_peptide_id:
                logger.warning(f"Peptide_ref '{peptide_ref}' not found in map")
                continue

            # Extract score values using helper function
            score_values = extract_score_values(sii)

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
            )

            psm_batch.append(psm)

            # Link PSM to peptide evidence via junction table
            pe_refs = sii.get("PeptideEvidenceRef", [])
            if not isinstance(pe_refs, list):
                pe_refs = [pe_refs]
            for pe_ref in pe_refs:
                pe_ref_id = pe_ref.get("peptideEvidence_ref", "")
                db_pe_id = pe_id_map.get(pe_ref_id)

                if db_pe_id:
                    junction = PSMPeptideEvidence(
                        psm_id=psm.id,
                        peptide_evidence_id=db_pe_id,
                    )
                    junction_batch.append(junction)

        logger.debug(f"Parsed {len(psm_batch)} PSMs and {len(junction_batch)} junctions so far")
    return psm_batch, junction_batch


def link_modifications(
    peptide_mods: dict[uuid.UUID, list[dict[str, Any]]],
) -> list[PeptideModification]:
    """
    Create PeptideModification records for all peptides with modifications.

    Args:
            session: SQLModel session
            peptide_mods: Mapping from database Peptide.id to list of modification data

    Returns:
            Number of modifications created
    """

    peptide_mod_batch = []

    for peptide_id, mods in peptide_mods.items():
        for mod in mods:
            # Extract UNIMOD ID
            unimod_id = extract_unimod_id(mod)

            # Skip modifications without valid UNIMOD ID
            if unimod_id is None:
                logger.warning(f"No UNIMOD ID found for modification: {mod}")
                continue

            location, residues = parse_modification_location(mod)

            # Skip modifications without valid location
            if location is None:
                logger.warning(f"No location found for modification: {mod}")
                continue

            # Create modification record
            peptide_mod = PeptideModification(
                peptide_id=peptide_id,
                unimod_id=unimod_id,
                position=location,
                modified_residue=residues or "",
            )
            peptide_mod_batch.append(peptide_mod)

    logger.debug(f"Created {len(peptide_mod_batch)} peptide modification records")
    return peptide_mod_batch


def parse_mzid_file(mzid_path: Path, project_accession: str) -> ParsedMzidData:
    """
    Parse an mzIdentML file and return all parsed data structures.

    This function performs pure parsing with no database operations.

    Args:
        mzid_path: Path to the mzIdentML file
        project_accession: PRIDE project accession

    Returns:
        ParsedMzidData containing all parsed records

    Raises:
        MzidParseError: If file cannot be read or parsed
    """
    # Validate file exists
    if not mzid_path.exists():
        error_msg = f"File not found: {mzid_path}"
        logger.error(error_msg)
        raise MzidParseError(error_msg)

    logger.info(f"Parsing mzID file: {mzid_path.name}")

    try:
        # Parse mzID file with retrieve_refs=False
        with mzid.MzIdentML(str(mzid_path), retrieve_refs=False) as reader:
            # Parse software and threshold metadata
            software_name, software_version = parse_software_info(reader)
            threshold_type, threshold_value = parse_threshold_info(reader)

            # Phase 0: Create mzid record
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

            # Phase 1: Parse DB sequences
            logger.debug("\nPhase 1: Parsing database sequences...")
            db_sequence_map = parse_db_sequences(reader)

            # Phase 2: Parse peptides
            logger.debug("\nPhase 2: Parsing peptides...")
            peptide_id_map, peptide_mods, peptides_batch = parse_peptides(reader)

            # Phase 3: Parse peptide evidence
            logger.debug("\nPhase 3: Parsing peptide evidence...")
            pe_id_map, peptide_evidence_batch = parse_peptide_evidence(reader, db_sequence_map)

            # Phase 4: Parse PSMs
            logger.debug("\nPhase 4: Parsing spectrum identification results...")
            psm_batch, junction_batch = parse_psms(
                reader,
                project_accession,
                mzid_file.id,
                peptide_id_map,
                pe_id_map,
            )

            # Phase 5: Link modifications
            logger.debug("\nPhase 5: Linking peptide modifications...")
            mod_batch = link_modifications(peptide_mods)

            # Return all parsed data
            parsed_data = ParsedMzidData(
                mzid_file=mzid_file,
                peptides=peptides_batch,
                peptide_modifications=mod_batch,
                peptide_evidence=peptide_evidence_batch,
                psms=psm_batch,
                psm_peptide_evidence_junctions=junction_batch,
            )

            logger.debug(
                f"Parsing complete: {len(peptides_batch)} peptides, "
                f"{len(mod_batch)} modifications, "
                f"{len(peptide_evidence_batch)} evidence, "
                f"{len(psm_batch)} PSMs"
            )

            return parsed_data

    except PyteomicsError as e:
        error_msg = f"Failed to parse mzID file: {e}"
        logger.error(error_msg, exc_info=True)
        raise MzidParseError(error_msg) from e


def import_mzid(mzid_path: Path, project_accession: str) -> ImportStats:
    """
    Import an mzIdentML file into the database.

    This function orchestrates parsing and database persistence:
    1. Parses the mzID file using parse_mzid_file()
    2. Persists all parsed data to the database in a single transaction

    Args:
        mzid_path: Path to the mzIdentML file
        project_accession: PRIDE project accession

    Returns:
        ImportStats object containing import statistics and status

    Raises:
        MzidParseError: If file cannot be read or parsed
        MzidImportError: If database operations fail
    """
    # Initialize stats tracker
    stats = ImportStats(
        file_name=mzid_path.name,
        project_accession=project_accession,
    )

    logger.info(f"Importing mzID file: {mzid_path.name}")

    try:
        # Step 1: Parse the mzID file (pure parsing, no DB operations)
        parsed_data = parse_mzid_file(mzid_path, project_accession)

        # Step 2: Persist everything to the database
        engine = load_db_engine()
        with Session(engine) as session:
            session.add(parsed_data.mzid_file)
            session.add_all(parsed_data.peptides)
            session.add_all(parsed_data.peptide_evidence)
            session.add_all(parsed_data.psms)
            session.add_all(parsed_data.psm_peptide_evidence_junctions)
            session.add_all(parsed_data.peptide_modifications)
            session.commit()

        # Update stats
        stats.peptide_count = len(parsed_data.peptides)
        stats.modification_count = len(parsed_data.peptide_modifications)
        stats.peptide_evidence_count = len(parsed_data.peptide_evidence)
        stats.psm_count = len(parsed_data.psms)
        stats.mark_complete()

        logger.info(stats.summary())
        return stats

    except MzidParseError as e:
        # Re-raise parsing errors with updated stats
        stats.mark_failed(str(e))
        raise
    except SQLAlchemyError as e:
        error_msg = f"Database error during import: {e}"
        logger.error(error_msg, exc_info=True)
        stats.mark_failed(error_msg)
        raise MzidImportError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error during import: {e}"
        logger.error(error_msg, exc_info=True)
        stats.mark_failed(error_msg)
        raise MzidImportError(error_msg) from e


if __name__ == "__main__":
    # Example usage
    SAMPLE_PROJECT = "PXD000001"

    for mzid_file in Path("experimental/mzid/").glob("*.mzid"):
        import_mzid(mzid_file, SAMPLE_PROJECT)
