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
) -> tuple[dict[str, uuid.UUID], dict[uuid.UUID, dict[str, list[tuple[int, str]]]], list[Peptide]]:
    """
    Parse peptide data from evidence and peptides DataFrames and extract modification information.
    This function processes two DataFrames (evidence and peptides) to create a comprehensive
    peptide dataset with associated modifications. It maps peptide sequences to unique identifiers,
    extracts modification details, and creates Peptide objects.
    Args:
        evidence (pd.DataFrame): DataFrame containing peptide evidence data with columns:
            - "Sequence": Amino acid sequence of the peptide
            - "Modifications": Comma-separated list of modifications applied to the peptide
            - "Modified sequence": Sequence representation with modifications encoded
        peptides (pd.DataFrame): DataFrame containing base peptide data with column:
            - "Sequence": Amino acid sequence of the peptide
    Returns:
        tuple[dict[str, uuid.UUID],
        dict[uuid.UUID, dict[str, list[tuple[int, str]]]], list[Peptide]]:
            - peptide_id_map: Dictionary mapping peptide sequences to their unique UUID identifiers
            - peptide_mods: Dictionary mapping peptide UUIDs to dict of modifications mapping to a
              list of tuples (position, residue)
            - peptides_batch: List of Peptide objects created from the peptides DataFrame
    Raises:
        None (implicitly handles missing data with defaults and empty DataFrames)
    Note:
        - Modifications marked as "Unmodified" are skipped
        - Modification information is extracted from modified sequences and mod lists
        - Multiple modifications for the same sequence are accumulated
    """
    peptide_id_map: dict[str, uuid.UUID] = {}
    peptide_mods: dict[uuid.UUID, dict[str, list[tuple[int, str]]]] = {}
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

        all_mods: dict[str, list[tuple[int, str]]] = {}
        for modifications, modified_sequence in evidences:
            if modifications:
                if modifications == "Unmodified":
                    continue
                mod_list: list[str] = clean_mod_list_of_numbers(modifications.split(","))
                modified_sequence = modified_sequence
                mods = extract_mods(modified_sequence, mod_list)

                for mod, mods_for_peptide in mods.items():
                    if mod not in all_mods:
                        all_mods[mod] = []
                    all_mods[mod].extend([m for m in mods_for_peptide if m not in all_mods[mod]])

        if all_mods:
            peptide_mods[peptide.id] = all_mods

    logger.debug(f"Created {len(peptides_batch)} peptide records")
    return peptide_id_map, peptide_mods, peptides_batch


def parse_peptide_evidence(peptides: pd.DataFrame) -> tuple[dict[str, list[uuid.UUID]], list[Any]]:
    """
    Parse peptide evidence from peptides DataFrame.
    This function processes a DataFrame containing peptide data to create peptide evidence
    records. It maps peptide sequences to lists of peptide evidence UUIDs and creates
    PeptideEvidence objects.
    Args:
        peptides (pd.DataFrame): DataFrame containing peptide data with columns:
            - "Sequence": Amino acid sequence of the peptide
            - "Amino acid before": Residue before the peptide in the protein sequence
            - "Amino acid after": Residue after the peptide in the protein sequence
            - "Proteins": Semicolon-separated list of protein accessions containing the peptide
            - "Leading razor protein": The primary protein accession for the peptide
            - "Start position": Start position of the peptide in the leading razor protein
            - "End position": End position of the peptide in the leading razor protein
    Returns:
        tuple[dict[str, list[uuid.UUID]], list[PeptideEvidence]]:
            - pe_id_map: Dictionary mapping peptide sequences to lists of peptide evidence UUIDs
            - peptide_evidence_batch: List of PeptideEvidence objects created from the peptides
                DataFrame
    Raises:
        None (implicitly handles missing data with defaults and empty DataFrames)
    """
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
    """
    Parse Peptide Spectrum Matches (PSMs) from evidence and summary DataFrames.
    This function processes evidence and summary DataFrames to create PSM records,
    link them to peptide evidence, and associate search modifications.
    Args:
        evidence (pd.DataFrame): DataFrame containing peptide evidence data with columns:
            - "Sequence": Amino acid sequence of the peptide
            - "Raw file": Identifier for the raw data file
            - "Charge": Charge state of the peptide
            - "m/z": Mass-to-charge ratio
            - "Mass": Calculated mass of the peptide
            - "MS/MS scan number": Scan number in the mass spectrometry data
        summary (pd.DataFrame): DataFrame containing summary data with columns:
            - "Raw file": Identifier for the raw data file
            - "Variable modifications": Comma-separated list of variable modifications
            - "Fixed modifications": Comma-separated list of fixed modifications
        project_accession (str): Accession identifier for the project
        peptide_id_map (dict[str, uuid.UUID]): Mapping of peptide sequences to their UUIDs
        pe_id_map (dict[str, list[uuid.UUID]]): Mapping of peptide sequences to lists of peptide
            evidence UUIDs
    Returns:
        tuple[list[PeptideSpectrumMatch], list[PSMPeptideEvidence], list[SearchModification]]:
            - psm_batch: List of PeptideSpectrumMatch records
            - junction_batch: List of PSMPeptideEvidence junction records
            - search_mod_batch: List of SearchModification records
    Raises:
        None (implicitly handles missing data with defaults and empty DataFrames)
    """
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
        unimod_id_list: list[int] = [
            lookup_unimod_id_by_name(mod)
            for mod in modification_list
            if lookup_unimod_id_by_name(mod) is not None
        ]

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
    peptide_mods: dict[uuid.UUID, dict[str, list[tuple[int, str]]]],
) -> list[PeptideModification]:
    """
    Convert peptide modifications data into PeptideModification records.
    This function processes a nested dictionary structure of peptide modifications
    and creates individual PeptideModification objects for each modification entry.
    Args:
        peptide_mods: A nested dictionary mapping peptide UUIDs to modification data.
                     Structure: {peptide_id: {mod_name: [(position, residue), ...]}}
                     - peptide_id (uuid.UUID): Unique identifier for the peptide
                     - mod_name (str): Name of the modification
                     - position (int): Position of the modification in the peptide
                     - residue (str): The modified amino acid residue
    Returns:
        list[PeptideModification]: A list of PeptideModification records, one for each
                                  position-residue pair in the input data.
    Raises:
        KeyError: If a modification name cannot be found in the Unimod database.
    """
    peptide_mod_batch: list[PeptideModification] = []

    for peptide_id, mods_dict in peptide_mods.items():
        for mod_name, positions_residues in mods_dict.items():
            for position, residue in positions_residues:
                modification_record = PeptideModification(
                    peptide_id=peptide_id,
                    unimod_id=lookup_unimod_id_by_name(mod_name),
                    name=mod_name,
                    position=position,
                    modified_residue=residue,
                )
                if modification_record.unimod_id is not None:
                    peptide_mod_batch.append(modification_record)

    logger.debug(f"Created {len(peptide_mod_batch)} peptide modification records")
    return peptide_mod_batch
