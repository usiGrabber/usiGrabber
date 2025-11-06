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
	Modification,
	MzidFile,
	Peptide,
	PeptideEvidence,
	PeptideModification,
	PeptideSpectrumMatch,
	Protein,
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


def get_or_create_protein(
	session: Session, accession: str, is_decoy: bool = False
) -> tuple[Protein, bool]:
	"""Get existing protein or create new one. Returns (protein, created)."""
	stmt = select(Protein).where(Protein.accession == accession)
	protein = session.exec(stmt).first()
	if not protein:
		protein = Protein(accession=accession, is_decoy=is_decoy)
		session.add(protein)
		session.flush()
		return protein, True
	return protein, False


def get_or_create_modification(
	session: Session, name: str | None, unimod_accession: str | None, mass_delta: float
) -> tuple[Modification, bool]:
	"""Get existing modification or create new one. Returns (modification, created)."""
	# Try to find by UNIMOD accession first if available
	if unimod_accession:
		stmt = select(Modification).where(Modification.unimod_accession == unimod_accession)
		modification = session.exec(stmt).first()
		if modification:
			return modification, False

	# Otherwise find by name and mass delta
	if name:
		stmt = select(Modification).where(
			Modification.name == name, Modification.mass_delta == mass_delta
		)
		modification = session.exec(stmt).first()
		if modification:
			return modification, False

	modification = Modification(name=name, unimod_accession=unimod_accession, mass_delta=mass_delta)
	session.add(modification)
	session.flush()

	return modification, True


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
						# Get or create the modification
						modification, created = get_or_create_modification(
							session,
							name=mod_data["name"],
							unimod_accession=mod_data["unimod_accession"],
							mass_delta=mod_data["mass_delta"],
						)
						if created:
							modification_count += 1

						assert modification.id is not None, (
							"Modification ID should be set after flush"
						)

						# Create peptide-modification link
						peptide_mod = PeptideModification(
							peptide_id=peptide.id,
							modification_id=modification.id,
							position=mod_data["position"],
							modified_residue=mod_data["residues"] or "",
						)
						session.add(peptide_mod)

					# Process proteins from PeptideEvidenceRef
					evidences = psm_item.get("PeptideEvidenceRef", [])
					if not isinstance(evidences, list):
						evidences = [evidences]

					for evidence in evidences:
						accession = evidence.get("accession", "")
						if accession:
							protein, created = get_or_create_protein(
								session,
								accession=accession,
								is_decoy=evidence.get("isDecoy", False),
							)
							if created:
								protein_count += 1

							assert protein.id is not None, "Protein ID should be set after flush"

							# Create peptide-protein mapping
							pep_ev = PeptideEvidence(
								peptide_id=peptide.id,
								protein_id=protein.id,
								start_position=evidence.get("start"),
								end_position=evidence.get("end"),
								pre_residue=evidence.get("pre"),
								post_residue=evidence.get("post"),
							)
							session.add(pep_ev)

					# Create PSM record
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
					psm_count += 1

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
