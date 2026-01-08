import uuid
from uuid import UUID

import pandas as pd

from usigrabber.db.engine import logger
from usigrabber.db.schema import IndexType
from usigrabber.file_parser.helpers import (
    clean_mod_list_of_numbers,
    create_search_mod_log_str,
    extract_mods,
    simple_mod_name,
)
from usigrabber.file_parser.models import (
    ModificationDict,
    ModifiedPeptideDict,
    ModifiedPeptideModificationJunctionDict,
    PeptideEvidenceDict,
    PeptideSpectrumMatchDict,
    PSMPeptideEvidenceDict,
    SearchModificationDict,
)
from usigrabber.file_parser.uuid_helpers import (
    generate_deterministic_modification_uuid,
    generate_deterministic_peptide_uuid,
)
from usigrabber.utils import lookup_unimod_id_by_name


def parse_peptides_and_modifications(
    evidence: pd.DataFrame,
) -> tuple[
    dict[str, uuid.UUID],
    list[ModifiedPeptideDict],
    list[ModificationDict],
    list[ModifiedPeptideModificationJunctionDict],
]:
    """
    Parse peptides and their modifications from evidence DataFrame.

    Processes evidence data to create modified peptide records with associated modifications.
    Deduplicates identical modified peptides and extracts modification details for each unique
    peptide-modification combination.

    Args:
        evidence (pd.DataFrame): DataFrame containing peptide data with columns:
            - "Sequence": Amino acid sequence
            - "Modifications": Comma-separated list of modifications
            - "Modified sequence": Sequence representation with modifications encoded

    Returns:
        tuple containing:
            - peptide_id_map: Maps peptide sequences to modified peptide UUIDs
            - peptides_batch: List of unique modified peptide records
            - modifications_batch: List of unique modification records
            - junction_batch: List of peptide-modification relationships
    """
    peptide_id_map: dict[str, uuid.UUID] = {}
    # Use dict for deduplication - same modified peptide UUID = same record
    peptides_dict: dict[uuid.UUID, ModifiedPeptideDict] = {}

    # Track modifications as we parse peptides
    modifications_dict: dict[uuid.UUID, ModificationDict] = {}
    junction_set: set[tuple[uuid.UUID, uuid.UUID]] = set()  # (modified_peptide_id, mod_id)

    evidence = evidence.get(
        ["Sequence", "Modifications", "Modified sequence"], default=pd.DataFrame()
    )
    evidence = evidence.drop_duplicates()

    for evidence_elem in evidence.iterrows():
        evidence_elem = evidence_elem[1]
        sequence: str = evidence_elem.get("Sequence", "")
        modifications: str = evidence_elem.get("Modifications", "")
        modified_sequence: str = evidence_elem.get("Modified sequence", "")
        if not sequence:
            continue
        parsed_mods = parse_modification_list(modifications, modified_sequence)
        modified_peptide_id = generate_deterministic_peptide_uuid(sequence, parsed_mods)

        peptide_dict: ModifiedPeptideDict = {
            "id": modified_peptide_id,
            "peptide_sequence": sequence,
        }
        if modified_peptide_id not in peptides_dict:
            peptides_dict[modified_peptide_id] = peptide_dict

            for mod in parsed_mods:
                mod_id = mod["id"]
                modifications_dict[mod_id] = mod
                # Add junction entry (set automatically deduplicates)
                junction_set.add((modified_peptide_id, mod_id))

        peptide_id_map[sequence] = modified_peptide_id

    peptides_batch = list(peptides_dict.values())
    modifications_batch = list(modifications_dict.values())
    junction_batch: list[ModifiedPeptideModificationJunctionDict] = [
        {"modified_peptide_id": mp_id, "modification_id": mod_id} for mp_id, mod_id in junction_set
    ]

    logger.debug(
        f"Created {len(peptides_batch)} unique modified peptide records, "
        f"{len(modifications_batch)} unique modifications, and "
        f"{len(junction_batch)} unique junction entries"
    )
    return peptide_id_map, peptides_batch, modifications_batch, junction_batch


def parse_modification_list(modifications, modified_sequence) -> list[ModificationDict]:
    """
    Parse modification list and modified sequence to extract modification details.
    This function processes a modification list string and a modified sequence string
    to create a mapping of modification names to their positions and residues in the peptide.
    Args:
        sequence (str): The unmodified amino acid sequence of the peptide
        modifications (str): Comma-separated list of modifications applied to the peptide
        modified_sequence (str): Sequence representation with modifications encoded
    Returns:
        list containing: A list of ModificationDict objects containing details about each
            modification, including: id, unimod_id OR name, location, and modified_residue.
    """
    parsed_mods: list[ModificationDict] = []
    if modifications == "Unmodified":
        return parsed_mods

    mod_dict = extract_mods(modified_sequence, clean_mod_list_of_numbers(modifications.split(",")))
    for mod_name, locations_residues in mod_dict.items():
        for location, residue in locations_residues:
            unimod_id = lookup_unimod_id_by_name(mod_name)
            mod_name = None if unimod_id else mod_name
            mod_uuid = generate_deterministic_modification_uuid(
                unimod_id, mod_name, location, residue
            )
            mod_record: ModificationDict = {
                "id": mod_uuid,
                "unimod_id": unimod_id,
                "name": mod_name,
                "location": location,
                "modified_residue": residue,
            }
            parsed_mods.append(mod_record)

    return parsed_mods


def parse_peptide_evidence(
    peptides: pd.DataFrame,
) -> tuple[dict[str, list[uuid.UUID]], list[PeptideEvidenceDict]]:
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
        tuple containing:
            - pe_id_map: Dictionary mapping peptide sequences to lists of peptide evidence UUIDs
            - peptide_evidence_batch: List of PeptideEvidence objects created from the peptides
                DataFrame
    """
    pe_id_map: dict[str, list[uuid.UUID]] = {}

    peptide_evidence_batch: list[PeptideEvidenceDict] = []
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

            pe_id = uuid.uuid4()
            peptide_evidence_dict: PeptideEvidenceDict = {
                "id": pe_id,
                "protein_accession": protein,
                "is_decoy": None,
                "start_position": start_position,
                "end_position": end_position,
                "pre_residue": pre_residue,
                "post_residue": post_residue,
            }
            peptide_evidence_batch.append(peptide_evidence_dict)
            pe_id_map[sequence].append(pe_id)

    logger.debug(f"Created {len(peptide_evidence_batch)} peptide evidence records")
    return pe_id_map, peptide_evidence_batch


def parse_psms(
    evidence: pd.DataFrame,
    summary: pd.DataFrame,
    project_accession: str,
    peptide_id_map: dict[str, uuid.UUID],
    pe_id_map: dict[str, list[uuid.UUID]],
) -> tuple[
    list[PeptideSpectrumMatchDict], list[PSMPeptideEvidenceDict], list[SearchModificationDict]
]:
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
        tuple containing:
            - psm_batch: List of PeptideSpectrumMatch records
            - junction_batch: List of PSMPeptideEvidence junction records
            - search_mod_batch: List of SearchModification records
    Raises:
        None (implicitly handles missing data with defaults and empty DataFrames)
    """
    psm_batch: list[PeptideSpectrumMatchDict] = []
    junction_batch: list[PSMPeptideEvidenceDict] = []
    search_mod_batch: list[SearchModificationDict] = []
    search_mod_counts = set[int]()

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
        modified_peptide_id: UUID | None = peptide_id_map.get(sequence)

        if not modified_peptide_id:
            logger.warning(f"Peptide_ref '{sequence}' not found in map")
            continue

        scan_id: int = psm_elem.get("MS/MS scan number", "")
        mass = psm_elem.get("Mass", 0)
        charge = psm_elem.get("Charge", 1)

        modification_list: list[str] = []
        raw_file = psm_elem.get("Raw file", "")
        for var_modifications, fixed_modified_sequence in summary_mod_map[raw_file]:
            var_mods = var_modifications
            fix_mods = fixed_modified_sequence
            if var_mods:
                modification_list.extend(var_mods.split(";"))
            if fix_mods:
                modification_list.extend(fix_mods.split(";"))
        unimod_id_list: list[int] = [
            lookup_unimod_id_by_name(simple_mod_name(mod))
            for mod in modification_list
            if lookup_unimod_id_by_name(simple_mod_name(mod)) is not None
        ]
        search_mod_counts.add(len(unimod_id_list))

        psm_id = uuid.uuid4()
        psm: PeptideSpectrumMatchDict = {
            "id": psm_id,
            "project_accession": project_accession,
            "modified_peptide_id": modified_peptide_id,
            "spectrum_id": None,
            "charge_state": psm_elem.get("Charge", None),
            "experimental_mz": psm_elem.get("m/z", None),
            "calculated_mz": mass / charge if charge else None,
            "pass_threshold": None,
            "index_type": IndexType.scan,
            "index_number": scan_id,
            "ms_run": psm_elem.get("Raw file", None),
        }
        psm_batch.append(psm)

        for unimod_id in unimod_id_list:
            # create search modification record
            search_mod: SearchModificationDict = {
                "id": uuid.uuid4(),
                "psm_id": psm["id"],
                "unimod_id": unimod_id,
            }
            search_mod_batch.append(search_mod)

        peptide_evidence_ids = pe_id_map.get(sequence, [])
        for pe_id in peptide_evidence_ids:
            junction: PSMPeptideEvidenceDict = {
                "id": uuid.uuid4(),
                "psm_id": psm["id"],
                "peptide_evidence_id": pe_id,
            }
            junction_batch.append(junction)

    logger.debug(f"Parsed {len(psm_batch)} PSMs and {len(junction_batch)} junctions")
    logger.debug(
        f"Each PSM is linked to {create_search_mod_log_str(search_mod_counts)} search modification(s)"
    )
    return psm_batch, junction_batch, search_mod_batch
