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
from datetime import datetime
from pathlib import Path
from typing import Any

from pyteomics import mzid
from sqlmodel import Session

from usigrabber.db.engine import load_db_engine
from usigrabber.db.schema import (
	MzidFile,
	Peptide,
	PeptideEvidence,
	PeptideModification,
	PeptideSpectrumMatch,
	PSMPeptideEvidence,
	create_db_and_tables,
)
from usigrabber.utils import get_unimod_db

logger = logging.getLogger(__name__)


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


def extract_unimod_id(mod_data: dict) -> int | None:
	"""
	Extract UNIMOD ID from modification cvParam data.

	Args:
		mod_data: Modification dictionary containing cvParam information

	Returns:
		UNIMOD ID as integer, or None if not found
	"""
	# Check if cvParam exists
	cv_params = mod_data.get("cvParam")

	if cv_params:
		# cvParam can be a list or a single dict
		if not isinstance(cv_params, list):
			cv_params = [cv_params]

		# Look for UNIMOD accession
		for param in cv_params:
			accession = param.get("accession", "")
			if "UNIMOD:" in accession and len(accession) > 7:
				try:
					# Extract number from "UNIMOD:35" format
					return int(accession.split(":")[-1])
				except (ValueError, IndexError):
					continue

	unimod_db = get_unimod_db()
	try:
		mod = unimod_db.get(mod_data.get("name", ""), False)
		if mod is not None:
			return mod.id
	except KeyError:
		return None

	return None


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
				logger.warning(f"Warning: peptide_ref '{peptide_ref}' not found in map")
				continue

			# Extract score values (anything that looks like a score)
			score_values: dict[str, Any] = {}
			for key, value in sii.items():
				# Include fields that look like scores, expectation values, etc.
				key_lower = key.lower()
				if any(term in key_lower for term in ["score", "value", "fdr", "qvalue", "expect"]):
					# Skip fields that are not actual score values
					if key in ["peptide_ref", "PeptideEvidenceRef", "passThreshold"]:
						continue
					score_values[key] = value

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
				logger.info(f"Warning: No UNIMOD ID found for modification: {mod}")
				continue

			# Get modification location and residue
			location = mod.get("location", 0)
			residues = mod.get("residues", "")

			# Convert residues to string if it's a list
			if isinstance(residues, list):
				residues = "".join(residues)

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


def import_mzid(mzid_path: Path, project_accession: str) -> None:
	"""
	Import an mzIdentML file into the database.

	Args:
		mzid_path: Path to the mzIdentML file
		project_accession: PRIDE project accession
	"""

	if not mzid_path.exists():
		logger.error(f"Error: File not found: {mzid_path}")
		return

	logger.info(f"Importing mzID file: {mzid_path.name}")

	engine = load_db_engine()
	create_db_and_tables(engine)

	with Session(engine) as session:
		# Create MzidFile record for provenance
		mzid_file = MzidFile(
			project_accession=project_accession,
			file_name=mzid_path.name,
			file_path=str(mzid_path.absolute()),
			software_name="software_name",
			software_version="software_version",
			threshold_type="threshold_type",
			threshold_value=0.01,
			creation_date=datetime.now(),
		)
		session.add(mzid_file)
		session.flush()

		logger.info(f"Created mzID file record (ID: {mzid_file.id})")

		# Parse mzID file with retrieve_refs=False
		with mzid.MzIdentML(str(mzid_path), retrieve_refs=False) as reader:
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

		logger.info(
			f"\n✅ Successfully imported:\n"
			f"  • {len(peptide_id_map):,} peptides\n"
			f"  • {mod_count:,} modifications\n"
			f"  • {len(pe_id_map):,} protein mappings\n"
			f"  • {psm_count:,} PSMs"
		)


if __name__ == "__main__":
	# Example usage
	SAMPLE_PROJECT = "PXD000001"

	for mzid_file in Path("experimental/mzid/").glob("*.mzid"):
		import_mzid(mzid_file, SAMPLE_PROJECT)
