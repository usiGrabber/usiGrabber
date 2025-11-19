import uuid
from typing import Any
from uuid import UUID

import pandas as pd

from usigrabber.db.engine import logger
from usigrabber.db.schema import (
    Peptide,
    PeptideEvidence,
    PeptideSpectrumMatch,
)
from usigrabber.file_parser.txt_zip.helpers import (
    clean_mod_list_of_numbers,
    extract_mod_positions_residues,
    extract_unimod_id,
    get_unimod_mod_dict,
)


def parse_peptides(
    evidence: pd.DataFrame, peptides: pd.DataFrame
) -> tuple[dict[str, uuid.UUID], dict[uuid.UUID, list[dict[str, Any]]], list[Peptide]]:
    peptide_id_map: dict[str, uuid.UUID] = {}
    peptide_mods: dict[uuid.UUID, list[dict[str, Any]]] = {}
    peptides_batch: list[Peptide] = []

    evidence = evidence.get(
        ["Sequence", "Modifications", "Modified sequence"], default=pd.DataFrame()
    )
    peptides = peptides.get(["Sequence"], default=pd.DataFrame())

    for peptide_elem in peptides.iterrows():
        peptide_elem = peptide_elem[1]
        sequence = peptide_elem.get("Sequence", "")
        if not sequence:
            continue
        peptide = Peptide(sequence=sequence, length=len(sequence))
        peptides_batch.append(peptide)
        peptide_id_map[sequence] = peptide.id

        evidences = evidence[evidence["Sequence"] == sequence]
        evidences = evidences.drop_duplicates()
        for modification_elem in evidences.iterrows():
            modification_elem = modification_elem[1]
            modifications = modification_elem.get("Modifications", "")
            if modifications:
                if modifications == "Unmodified":
                    pass
                else:
                    mod_list: list[str] = modifications.split(",")
                    mod_list = clean_mod_list_of_numbers(mod_list)
                    modified_sequence: str = modification_elem.get("Modified sequence", "")
                    mods = extract_mod_positions_residues(modified_sequence, mod_list)
                    mods = get_unimod_mod_dict(mods)

                    peptide_mods[peptide.id] = []
                    for mod in mods:
                        for position in mods[mod]:
                            peptide_mods[peptide.id].append(
                                dict(modification=mod, position=position)
                            )

    logger.debug(f"Created {len(peptides_batch)} peptide records")
    return peptide_id_map, peptide_mods, peptides_batch


def parse_peptide_evidence(peptides: pd.DataFrame) -> tuple[dict[str, list[uuid.UUID]], list[Any]]:
    pe_id_map: dict[str, list[uuid.UUID]] = {}

    peptide_evidence_batch = []

    peptides = peptides.get(
        key=[
            "Sequence",
            "Amino acid before",
            "Amino acid after",
            "Proteins",
            "Leading razor protein",
            "Start position",
            "End position",
        ],
        default=pd.DataFrame(),
    )
    peptides = peptides.astype({"Start position": int, "End position": int})

    for peptide_row in peptides.iterrows():
        peptide_row = peptide_row[1]
        sequence: str = peptide_row.get("Sequence", "")

        razor_protein: str = peptide_row.get("Leading razor protein", "")
        razor_protein = razor_protein.split(".")[0]
        proteins: str = peptide_row.get("Proteins", "")
        protein_list = proteins.split(";")
        protein_list = [p.split(".")[0] for p in protein_list]

        for protein in protein_list:
            peptide_evidence: PeptideEvidence = PeptideEvidence(
                protein_accession=protein,
                is_decoy=None,
                start_position=peptide_row.get("Start position", None)
                if protein == razor_protein
                else None,
                end_position=peptide_row.get("End position", None)
                if protein == razor_protein
                else None,
                pre_residue=peptide_row.get("Amino acid before", None)
                if protein == razor_protein
                else None,
                post_residue=peptide_row.get("Amino acid after", None)
                if protein == razor_protein
                else None,
            )
            peptide_evidence_batch.append(peptide_evidence)
            pe_id_map[sequence].append(peptide_evidence.id)

    return pe_id_map, peptide_evidence_batch


def parse_psms(
    evidence: pd.DataFrame,
    summary: pd.DataFrame,
    project_accession: str,
    peptide_id_map: dict[str, uuid.UUID],
    pe_id_map: dict[str, list[uuid.UUID]],
) -> tuple[list[Any], list[Any]]:
    psm_batch = []
    junction_batch = []

    evidence = evidence.get(
        ["Sequence", "Raw file", "Charge", "m/z", "Mass", "MS/MS scan number"],
        default=pd.DataFrame(),
    )
    evidence = evidence.drop_duplicates()
    evidence = evidence[evidence["MS/MS scan number"]]

    summary = summary.get(
        ["Raw file", "Variable modifications", "Fixed modifications"],
        default=pd.DataFrame(),
    )

    for psm_elem in evidence.iterrows():
        psm_elem = psm_elem[1]

        sequence: str = psm_elem.get("Sequence", "")
        db_peptide_id: UUID = peptide_id_map.get(sequence)

        scan_id: str = psm_elem.get("MS/MS scan number", "")
        mass = psm_elem.get("Mass", 0)
        charge = psm_elem.get("Charge", 1)

        summary = summary[summary["Raw file"] == psm_elem.get("Raw file", "")]
        modification_list: list[str] = []
        for summary_elem in summary.iterrows():
            summary_elem = summary_elem[1]
            var_mods = summary_elem.get("Variable modifications", "")
            fix_mods = summary_elem.get("Fixed modifications", "")
            if var_mods:
                modification_list.extend(var_mods.split(";"))
            if fix_mods:
                modification_list.extend(fix_mods.split(";"))
        for mod in modification_list:
            mod = extract_unimod_id(mod)

        psm = PeptideSpectrumMatch(
            project_accession=project_accession,
            peptide_id=db_peptide_id,
            spectrum_id=scan_id,
            charge_state=psm_elem.get("Charge", None),
            experimental_mz=psm_elem.get("m/z", None),
            calculated_mz=mass / charge if charge else None,
            pass_threshold=None,
            # tested_for = modification_list
            # experiment_name = psm_elem.get("Raw file", None),
        )

        psm_batch.append(psm)

        peptide_evidence_ids = pe_id_map.get(sequence, [])
        for pe_id in peptide_evidence_ids:
            junction = dict(
                psm_id=psm.id,
                peptide_evidence_id=pe_id,
            )
            junction_batch.append(junction)

    return psm_batch, junction_batch


def link_modifications(peptide_mods: dict[uuid.UUID, list[dict[str, Any]]]) -> list[Any]:
    peptide_mod_batch = []

    for peptide_id, mods in peptide_mods.items():
        for mod in mods:
            modification_record = dict(
                peptide_id=peptide_id,
                unimod_id=mod["modification"],
                position=mod["position"],
            )
            peptide_mod_batch.append(modification_record)

    logger.debug(f"Created {len(peptide_mod_batch)} peptide modification records")
    return peptide_mod_batch
