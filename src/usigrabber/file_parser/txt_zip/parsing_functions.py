import uuid
from typing import Any
from uuid import UUID

import pandas as pd

from usigrabber.db.engine import logger
from usigrabber.db.schema import (
    IndexType,
    Peptide,
    PeptideEvidence,
    PeptideModification,
    PeptideSpectrumMatch,
    PSMPeptideEvidence,
    SearchModification,
)
from usigrabber.file_parser.txt_zip.helpers import (
    clean_mod_list_of_numbers,
    extract_mods,
)
from usigrabber.utils import lookup_unimod_id_by_name


def parse_peptides(
    evidence: pd.DataFrame, peptides: pd.DataFrame
) -> tuple[dict[str, uuid.UUID], dict[uuid.UUID, list[list[tuple[int, str, str]]]], list[Peptide]]:
    peptide_id_map: dict[str, uuid.UUID] = {}
    peptide_mods: dict[uuid.UUID, list[list[tuple[int, str, str]]]] = {}
    peptides_batch: list[Peptide] = []

    evidence = evidence.get(
        ["Sequence", "Modifications", "Modified sequence"], default=pd.DataFrame()
    )
    peptides = peptides.get(["Sequence"], default=pd.DataFrame())

    evidence_mod_map: dict[str, set[tuple[str, str]]] = {}
    for evidence_elem in evidence.iterrows():
        evidence_elem = evidence_elem[1]
        sequence = evidence_elem.get("Sequence", "")
        modifications: str = evidence_elem.get("Modifications", "")
        modified_sequence: str = evidence_elem.get("Modified sequence", "")
        if sequence:
            if sequence not in evidence_mod_map:
                evidence_mod_map[sequence] = set()
            evidence_mod_map[sequence].add((modifications, modified_sequence))

    for peptide_elem in peptides.iterrows():
        peptide_elem = peptide_elem[1]
        sequence = peptide_elem.get("Sequence", "")
        if not sequence:
            continue
        peptide = Peptide(sequence=sequence, length=len(sequence))
        peptides_batch.append(peptide)
        peptide_id_map[sequence] = peptide.id

        evidences = evidence_mod_map.get(sequence, set())

        for modified_elem in evidences:
            modifications = modified_elem[0]
            if modifications:
                if modifications == "Unmodified":
                    continue
                mod_list: list[str] = modifications.split(",")
                mod_list = clean_mod_list_of_numbers(mod_list)
                modified_sequence = modified_elem[1]
                mods = extract_mods(modified_sequence, mod_list)

                mods_for_peptide: list[tuple[int, str, str]] = []
                for mod in mods:
                    for position_residues in mods[mod]:
                        mods_for_peptide.append((*position_residues, mod))
                peptide_mods.setdefault(peptide.id, []).append(mods_for_peptide)

    logger.debug(f"Created {len(peptides_batch)} peptide records")
    return peptide_id_map, peptide_mods, peptides_batch


def parse_peptide_evidence(peptides: pd.DataFrame) -> tuple[dict[str, list[uuid.UUID]], list[Any]]:
    pe_id_map: dict[str, list[uuid.UUID]] = {}

    peptide_evidence_batch = []

    peptides = peptides.get(
        [
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

    for peptide_row in peptides.iterrows():
        peptide_row = peptide_row[1]
        sequence: str = peptide_row.get("Sequence", "")
        pe_id_map[sequence] = []

        razor_protein: str = peptide_row.get("Leading razor protein", "")
        razor_protein = razor_protein.split(".")[0]
        proteins: str = peptide_row.get("Proteins", "")
        protein_list = str(proteins).split(";")
        protein_list = [p.split(".")[0] for p in protein_list]

        for protein in protein_list:
            start_position = peptide_row.get("Start position", None)
            start_position = (
                int(start_position) if (start_position and protein == razor_protein) else None
            )
            end_position = peptide_row.get("End position", None)
            end_position = (
                int(end_position) if (end_position and protein == razor_protein) else None
            )
            pre_residue = peptide_row.get("Amino acid before", None)
            pre_residue = pre_residue if (pre_residue and protein == razor_protein) else None
            post_residue = peptide_row.get("Amino acid after", None)
            post_residue = post_residue if (post_residue and protein == razor_protein) else None

            peptide_evidence: PeptideEvidence = PeptideEvidence(
                protein_accession=protein,
                is_decoy=None,
                start_position=start_position,
                end_position=end_position,
                pre_residue=pre_residue,
                post_residue=post_residue,
            )
            peptide_evidence_batch.append(peptide_evidence)
            pe_id_map[sequence].append(peptide_evidence.id)

    logger.debug(f"Created {len(peptide_evidence_batch)} peptide evidence records")
    return pe_id_map, peptide_evidence_batch


def parse_psms(
    evidence: pd.DataFrame,
    summary: pd.DataFrame,
    project_accession: str,
    peptide_id_map: dict[str, uuid.UUID],
    pe_id_map: dict[str, list[uuid.UUID]],
) -> tuple[list[PeptideSpectrumMatch], list[PSMPeptideEvidence], list[SearchModification]]:
    psm_batch: list[PeptideSpectrumMatch] = []
    junction_batch: list[PSMPeptideEvidence] = []
    search_mod_batch: list[SearchModification] = []

    evidence = evidence.get(
        ["Sequence", "Raw file", "Charge", "m/z", "Mass", "MS/MS scan number"],
        default=pd.DataFrame(),
    )
    evidence = evidence.drop_duplicates()
    evidence = evidence[evidence["MS/MS scan number"].notna()]

    summary = summary.get(
        ["Raw file", "Variable modifications", "Fixed modifications"],
        default=pd.DataFrame(),
    )

    summary_mod_map: dict[str, set[tuple[str, str]]] = {}
    for summary_elem in summary.iterrows():
        summary_elem = summary_elem[1]
        raw_file: str = summary_elem.get("Raw file", "")
        var_modifications: str = summary_elem.get("Variable modifications", "")
        fixed_modified_sequence: str = summary_elem.get("Fixed modifications", "")
        if raw_file:
            if raw_file not in summary_mod_map:
                summary_mod_map[raw_file] = set()
            summary_mod_map[raw_file].add((var_modifications, fixed_modified_sequence))

    for psm_elem in evidence.iterrows():
        psm_elem = psm_elem[1]

        sequence: str = psm_elem.get("Sequence", "")
        db_peptide_id: UUID = peptide_id_map.get(sequence)

        scan_id: int = psm_elem.get("MS/MS scan number", "")
        mass = psm_elem.get("Mass", 0)
        charge = psm_elem.get("Charge", 1)

        modification_list: list[str] = []
        raw_file = psm_elem.get("Raw file", "")
        for summary_elem in summary_mod_map[raw_file]:
            var_mods = summary_elem[0]
            fix_mods = summary_elem[1]
            if var_mods:
                modification_list.extend(var_mods.split(";"))
            if fix_mods:
                modification_list.extend(fix_mods.split(";"))
        unimod_id_list = []
        for mod in modification_list:
            unimod_id_list.append(lookup_unimod_id_by_name(mod))

        psm = PeptideSpectrumMatch(
            project_accession=project_accession,
            peptide_id=db_peptide_id,
            spectrum_id=None,
            charge_state=psm_elem.get("Charge", None),
            experimental_mz=psm_elem.get("m/z", None),
            calculated_mz=mass / charge if charge else None,
            pass_threshold=None,
            index_type=IndexType.scan,
            index_number=scan_id,
            ms_run=psm_elem.get("Raw file", None),
        )

        psm_batch.append(psm)

        for unimod_id in unimod_id_list:
            search_mod = SearchModification(
                psm_id=psm.id,
                unimod_id=unimod_id,
            )
            search_mod_batch.append(search_mod)

        peptide_evidence_ids = pe_id_map.get(sequence, [])
        for pe_id in peptide_evidence_ids:
            junction = PSMPeptideEvidence(
                psm_id=psm.id,
                peptide_evidence_id=pe_id,
            )
            junction_batch.append(junction)

    logger.debug(f"Parsed {len(psm_batch)} PSMs and {len(junction_batch)} junctions")
    return psm_batch, junction_batch, search_mod_batch


def link_modifications(
    peptide_mods: dict[uuid.UUID, list[list[tuple[int, str, str]]]],
) -> list[PeptideModification]:
    peptide_mod_batch: list[PeptideModification] = []

    for peptide_id, mods_list in peptide_mods.items():
        for mods in mods_list:
            for position_residues_name in mods:
                modification_record = PeptideModification(
                    peptide_id=peptide_id,
                    unimod_id=lookup_unimod_id_by_name(position_residues_name[2]),
                    name=position_residues_name[2],
                    position=position_residues_name[0],
                    modified_residue=position_residues_name[1],
                )
                peptide_mod_batch.append(modification_record)

    logger.debug(f"Created {len(peptide_mod_batch)} peptide modification records")
    return peptide_mod_batch
