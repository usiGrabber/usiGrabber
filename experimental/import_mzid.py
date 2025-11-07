#!/usr/bin/env python3
"""
Import the two example mzID files into the database - SIMPLIFIED VERSION.

Prerequisites:
    Run the seed script first to create the database and projects:
    uv run python src/usigrabber/db/seed.py
"""

from datetime import datetime
from pathlib import Path

from pyteomics import mzid
from rich.console import Console
from sqlmodel import Session, select

from usigrabber.db import create_db_and_tables, load_db_engine
from usigrabber.db.schema import (
	MzidFile,
	Peptide,
	PeptideEvidence,
	PeptideModification,
	PeptideSpectrumMatch,
	PSMPeptideEvidence,
)

console = Console()

# Example mzID files to import
MZID_FILES = [
	"experimental/mzid/Fetal_Heartcombined_fdr_peptide_threshold_coord-1.2.mzid",
	"experimental/mzid/OTE0019_York_060813_JH16_F119502.mzid",
]
PROJECT_ACCESSION = "PXD000001"


def get_or_create_peptide(session: Session, sequence: str) -> tuple[Peptide, bool]:
	"""Get existing peptide or create new one. Returns (peptide, created)."""
	stmt = select(Peptide).where(Peptide.sequence == sequence)
	peptide = session.exec(stmt).first()
	if not peptide:
		peptide = Peptide(sequence=sequence, length=len(sequence))
		session.add(peptide)
		session.flush()
		return peptide, True
	return peptide, False


def extract_unimod_id(unimod_accession: str | None) -> int | None:
	"""Extract numeric UNIMOD ID from accession string like 'UNIMOD:35'."""
	if not unimod_accession:
		return None
	try:
		# Handle formats like "UNIMOD:35" or "35"
		if ":" in unimod_accession:
			return int(unimod_accession.split(":")[-1])
		return int(unimod_accession)
	except (ValueError, IndexError):
		return None


def parse_modifications(psm_item: dict) -> list[dict]:
	"""Extract modification information from PSM."""
	modifications = []

	# Look for modifications in the PSM data
	if "Modification" in psm_item:
		mods = psm_item["Modification"]
		if not isinstance(mods, list):
			mods = [mods]

		for mod in mods:
			# Get basic mod info
			position = mod.get("location", 0)
			mass_delta = mod.get("monoisotopicMassDelta", 0.0)
			residues = mod.get("residues", "")

			# Convert residues to string if it's a list
			if isinstance(residues, list):
				residues = "".join(residues)

			# Extract name and UNIMOD accession from cvParam
			name = None
			unimod_accession = None

			if "cvParam" in mod:
				params = mod["cvParam"]
				if not isinstance(params, list):
					params = [params]

				for param in params:
					accession = param.get("accession", "")
					if "UNIMOD" in accession:
						unimod_accession = accession
						name = param.get("name")
						break

			modifications.append(
				{
					"name": name,
					"unimod_accession": unimod_accession,
					"mass_delta": mass_delta,
					"position": position,
					"residues": residues,
				}
			)

	return modifications


def import_mzid(mzid_path: str, project_accession: str):
	"""Import mzID file - simple version."""

	mzid_path_obj = Path(mzid_path)
	if not mzid_path_obj.exists():
		console.print(f"[red]Error: File not found: {mzid_path_obj}[/red]")
		return

	console.print(f"\n🔬 Importing: {mzid_path_obj.name}")

	engine = load_db_engine()
	create_db_and_tables(engine)

	with Session(engine) as session:
		# Create MzidFile record
		mzid_file = MzidFile(
			project_accession=project_accession,
			file_name=mzid_path_obj.name,
			file_path=str(mzid_path_obj.absolute()),
			threshold_type="FDR",
			threshold_value=0.01,
			creation_date=datetime.now(),
		)
		session.add(mzid_file)
		session.flush()

		console.print(f"✓ Created mzID file record (ID: {mzid_file.id})")

		# Parse PSMs
		psm_count = 0
		peptide_count = 0
		protein_count = 0
		modification_count = 0

		console.print("📊 Parsing PSMs...")

		with mzid.read(str(mzid_path_obj)) as reader:
			for spectrum_result in reader:
				# Get spectrum ID
				spectrum_id = spectrum_result.get("spectrumID", "")

				# Get PSM items (can be a list)
				psm_items = spectrum_result.get("SpectrumIdentificationItem", [])
				if not isinstance(psm_items, list):
					psm_items = [psm_items]

				for psm_item in psm_items:
					# Get peptide sequence
					sequence = psm_item.get("PeptideSequence", "")
					if not sequence:
						continue

					# Create or get peptide
					peptide, created = get_or_create_peptide(session, sequence)
					if created:
						peptide_count += 1

					assert peptide.id is not None, "Peptide ID should be set after flush"

					# Parse and create modifications
					mods = parse_modifications(psm_item)
					for mod_data in mods:
						# Extract UNIMOD ID from the accession
						unimod_id = extract_unimod_id(mod_data["unimod_accession"])

						# Skip modifications without a valid UNIMOD ID
						if unimod_id is None:
							continue

						modification_count += 1

						# Create peptide-modification link with UNIMOD ID
						peptide_mod = PeptideModification(
							peptide_id=peptide.id,
							unimod_id=unimod_id,
							position=mod_data["position"],
							modified_residue=mod_data["residues"] or "",
						)
						session.add(peptide_mod)

					# Create PSM record first
					psm_record = PeptideSpectrumMatch(
						project_accession=project_accession,
						mzid_file_id=mzid_file.id,
						peptide_id=peptide.id,
						spectrum_id=spectrum_id,
						charge_state=psm_item.get("chargeState", 0),
						experimental_mz=psm_item.get("experimentalMassToCharge", 0.0),
						calculated_mz=psm_item.get("calculatedMassToCharge", 0.0),
						score_values={
							key: value
							for key, value in psm_item.items()
							if any(term in key.lower() for term in ["fdr", "score", "value"])
							and key != "PeptideSequence"
						},
						rank=psm_item.get("rank", 1),
						pass_threshold=psm_item.get("passThreshold", False),
						is_decoy=False,
					)
					session.add(psm_record)
					session.flush()  # Get PSM ID for junction table
					psm_count += 1

					# Process proteins from PeptideEvidenceRef and link to PSM
					evidences = psm_item.get("PeptideEvidenceRef", [])
					if not isinstance(evidences, list):
						evidences = [evidences]

					for evidence in evidences:
						accession = evidence.get("accession", "")
						is_decoy = evidence.get("isDecoy", False)
						if accession:
							# Create peptide evidence (protein mapping)
							pep_ev = PeptideEvidence(
								protein_accession=accession,
								isDecoy=is_decoy,
								start_position=evidence.get("start"),
								end_position=evidence.get("end"),
								pre_residue=evidence.get("pre"),
								post_residue=evidence.get("post"),
							)
							session.add(pep_ev)
							session.flush()  # Get the ID for junction table
							protein_count += 1

							assert pep_ev.id is not None, (
								"PeptideEvidence ID should be set after flush"
							)
							assert psm_record.id is not None, "PSM ID should be set after flush"

							# Link PSM to peptide evidence via junction table
							psm_pep_ev = PSMPeptideEvidence(
								psm_id=psm_record.id,
								peptide_evidence_id=pep_ev.id,
							)
							session.add(psm_pep_ev)

					# Commit every 100 PSMs
					if psm_count % 100 == 0:
						session.commit()
						console.print(f"  Processing... {psm_count:,} PSMs")

		# Final commit
		session.commit()

		console.print(
			f"\n✅ Imported {psm_count:,} PSMs, {peptide_count:,} peptides, "
			f"{protein_count:,} proteins, {modification_count:,} modifications"
		)


if __name__ == "__main__":
	for mzid_file in MZID_FILES:
		try:
			import_mzid(mzid_file, PROJECT_ACCESSION)
		except Exception as e:
			console.print(f"[red]❌ Error: {e}[/red]")

	console.print("\n[bold green]✅ Done![/bold green]")
