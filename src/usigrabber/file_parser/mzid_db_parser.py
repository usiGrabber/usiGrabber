"""
mzID Database Parser

Parses mzIdentML files using pyteomics and populates the database with:
- Peptides (one per mzID Peptide element, not deduplicated)
- Peptide Modifications (UNIMOD-based)
- Peptide Evidence (protein mappings)
- Peptide Spectrum Matches (PSMs)
- PSM-PeptideEvidence junction records

Uses retrieve_refs=False to avoid handling deduplication in the code.
Implements streaming parsing with periodic commits for large files.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

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
        logger.info(f"Parsed software: {software_name} v{software_version}")
        return software_name, software_version

    return None, None


def parse_threshold_info(reader: mzid.MzIdentML) -> tuple[str, float | None]:
    """
    Parse threshold information from SpectrumIdentificationProtocol.

    Args:
            reader: MzIdentML reader instance

    Returns:
            Tuple of (threshold_type, threshold_value)
    """
    threshold_type = "unknown"
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

            name = param.get("name", "unknown")
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

    logger.info(f"Parsed {len(db_sequence_map)} database sequences")
    return db_sequence_map


def parse_peptides(
    session: Session,
    reader: mzid.MzIdentML,
) -> tuple[dict[str, int], dict[int, list[dict[str, Any]]]]:
    """
    Parse Peptide elements and store in database.
    Creates a new Peptide record for each mzID Peptide element.

    Args:
            session: SQLModel session
            reader: MzIdentML reader instance

    Returns:
            Tuple of:
            - peptide_id_map: Maps mzID peptide IDs to database Peptide.id
            - peptide_mods: Maps database Peptide.id to list of modification data
    """

    peptide_id_map: dict[str, int] = {}
    peptide_mods: dict[int, list[dict[str, Any]]] = {}
    peptides_created = 0

    for peptide_elem in reader.iterfind("Peptide"):
        mzid_peptide_id = peptide_elem.get("id", "")
        sequence = peptide_elem.get("PeptideSequence", "")

        if not mzid_peptide_id or not sequence:
            continue

        peptide = Peptide(sequence=sequence, length=len(sequence))
        session.add(peptide)
        session.flush()
        peptides_created += 1

        assert peptide.id is not None, "Peptide ID should be set after flush"
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

    logger.info(f"Created {peptides_created} peptide records")
    return peptide_id_map, peptide_mods


def parse_peptide_evidence(
    session: Session,
    reader: mzid.MzIdentML,
    db_sequence_map: dict[str, str],
) -> dict[str, int]:
    """
    Parse PeptideEvidence elements and store in database.

    Args:
            session: SQLModel session
            reader: MzIdentML reader instance
            db_sequence_map: Mapping from DBSequence IDs to protein accessions

    Returns:
            Dictionary mapping mzID peptide evidence IDs to database PeptideEvidence.id
    """

    pe_id_map: dict[str, int] = {}
    pe_created = 0

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
            is_decoy=pe_elem.get("isDecoy", False),
            start_position=pe_elem.get("start"),
            end_position=pe_elem.get("end"),
            pre_residue=pe_elem.get("pre"),
            post_residue=pe_elem.get("post"),
        )

        session.add(peptide_evidence)
        session.flush()
        pe_created += 1

        assert peptide_evidence.id is not None, "PeptideEvidence ID should be set after flush"
        pe_id_map[mzid_pe_id] = peptide_evidence.id

    logger.info(f"Created {pe_created} peptide evidence records")
    return pe_id_map


def parse_psms(
    session: Session,
    reader: mzid.MzIdentML,
    project_accession: str,
    mzid_file_id: int,
    peptide_id_map: dict[str, int],
    pe_id_map: dict[str, int],
) -> int:
    """
    Parse SpectrumIdentificationResult elements and store PSMs in database.

    Args:
            session: SQLModel session
            reader: MzIdentML reader instance
            project_accession: PRIDE project accession
            mzid_file_id: Database ID of the MzidFile record
            peptide_id_map: Mapping from mzID peptide IDs to database Peptide.id
            pe_id_map: Mapping from mzID peptide evidence IDs to database PeptideEvidence.id

    Returns:
            Number of PSMs created
    """

    psm_count = 0

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
                charge_state=sii.get("chargeState", 0),
                experimental_mz=sii.get("experimentalMassToCharge", 0.0),
                calculated_mz=sii.get("calculatedMassToCharge", 0.0),
                score_values=score_values if score_values else None,
                rank=sii.get("rank", 1),
                pass_threshold=sii.get("passThreshold", False),
            )

            session.add(psm)
            session.flush()
            psm_count += 1

            assert psm.id is not None, "PSM ID should be set after flush"

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
                    session.add(junction)

            # Commit periodically to avoid memory issues
            if psm_count % 100 == 0:
                session.commit()
                logger.info(f"Processed {psm_count} PSMs...")

    logger.info(f"Created {psm_count} PSM records")
    return psm_count


def link_modifications(
    session: Session,
    peptide_mods: dict[int, list[dict[str, Any]]],
) -> int:
    """
    Create PeptideModification records for all peptides with modifications.

    Args:
            session: SQLModel session
            peptide_mods: Mapping from database Peptide.id to list of modification data

    Returns:
            Number of modifications created
    """

    mod_count = 0

    for peptide_id, mods in peptide_mods.items():
        for mod in mods:
            # Extract UNIMOD ID
            unimod_id = extract_unimod_id(mod)

            # Skip modifications without valid UNIMOD ID
            if unimod_id is None:
                logger.warning(f"No UNIMOD ID found for modification: {mod}")
                continue

            location, residues = parse_modification_location(mod)

            # Create modification record
            peptide_mod = PeptideModification(
                peptide_id=peptide_id,
                unimod_id=unimod_id,
                position=location,
                modified_residue=residues or "",
            )

            session.add(peptide_mod)
            mod_count += 1

    logger.info(f"Created {mod_count} peptide modification records")
    return mod_count


def import_mzid(mzid_path: Path, project_accession: str) -> ImportStats:
    """
    Import an mzIdentML file into the database.

    Implements streaming parsing with periodic commits to handle large files efficiently.
    Raises exceptions on errors.

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

    # Validate file exists
    if not mzid_path.exists():
        error_msg = f"File not found: {mzid_path}"
        logger.error(error_msg)
        stats.mark_failed(error_msg)
        raise MzidParseError(error_msg)

    logger.info(f"Importing mzID file: {mzid_path.name}")

    engine = load_db_engine()

    try:
        with Session(engine) as session:
            # Parse mzID file with retrieve_refs=False
            with mzid.MzIdentML(str(mzid_path), retrieve_refs=False) as reader:
                # Parse software and threshold metadata
                software_name, software_version = parse_software_info(reader)
                threshold_type, threshold_value = parse_threshold_info(reader)

                # Phase 0: Create mzid record and retrieve metadata
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
                session.add(mzid_file)
                session.flush()

                logger.info(f"Created mzID file record (ID: {mzid_file.id})")

                # Phase 1: Parse DB sequences
                logger.info("\nPhase 1: Parsing database sequences...")
                db_sequence_map = parse_db_sequences(reader)

                # Phase 2: Parse peptides
                logger.info("\nPhase 2: Parsing peptides...")
                peptide_id_map, peptide_mods = parse_peptides(session, reader)

                # Phase 3: Parse peptide evidence
                logger.info("\nPhase 3: Parsing peptide evidence...")
                pe_id_map = parse_peptide_evidence(session, reader, db_sequence_map)

                # Phase 4: Parse PSMs
                logger.info("\nPhase 4: Parsing spectrum identification results...")
                assert mzid_file.id is not None, "MzidFile ID should be set after flush"
                psm_count = parse_psms(
                    session,
                    reader,
                    project_accession,
                    mzid_file.id,
                    peptide_id_map,
                    pe_id_map,
                )

                # Phase 5: Link modifications
                logger.info("\nPhase 5: Linking peptide modifications...")
                mod_count = link_modifications(session, peptide_mods)

                # Final commit
                session.commit()

                # Update stats
                stats.peptide_count = len(peptide_id_map)
                stats.modification_count = mod_count
                stats.peptide_evidence_count = len(pe_id_map)
                stats.psm_count = psm_count
                stats.mark_complete()

            logger.info(stats.summary())
            return stats

    except PyteomicsError as e:
        error_msg = f"Failed to parse mzID file: {e}"
        logger.error(error_msg, exc_info=True)
        stats.mark_failed(error_msg)
        raise MzidParseError(error_msg) from e
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
