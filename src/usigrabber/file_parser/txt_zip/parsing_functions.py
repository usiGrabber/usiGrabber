import uuid
from uuid import UUID

import pandas as pd

from usigrabber.db.engine import logger
from usigrabber.db.schema import IndexType
from usigrabber.file_parser.models import (
    ModificationDict,
    ModifiedPeptideDict,
    ModifiedPeptideModificationJunctionDict,
    PeptideEvidenceDict,
    PeptideSpectrumMatchDict,
    PSMPeptideEvidenceDict,
    SearchModificationDict,
)
from usigrabber.file_parser.txt_zip.helpers import (
    clean_mod_list_of_numbers,
    extract_mods,
    simple_mod_name,
)
from usigrabber.file_parser.uuid_helpers import (
    generate_deterministic_modification_uuid,
    generate_deterministic_peptide_uuid,
)
from usigrabber.utils import lookup_unimod_id_by_name


def parse_peptides(
    evidence: pd.DataFrame, peptides: pd.DataFrame
) -> tuple[
    dict[str, uuid.UUID],
    list[ModifiedPeptideDict],
    list[ModificationDict],
    list[ModifiedPeptideModificationJunctionDict],
]:
    """
    Parse peptide data from evidence and peptides DataFrames and extract modification information.

    This function processes two DataFrames (evidence and peptides) to create ModifiedPeptide
    and Modification records. It maps peptide sequences to unique modified peptide identifiers,
    extracts modification details, and creates junction entries.

    Args:
        evidence (pd.DataFrame): DataFrame containing peptide evidence data with columns:
            - "Sequence": Amino acid sequence of the peptide
            - "Modifications": Comma-separated list of modifications applied to the peptide
            - "Modified sequence": Sequence representation with modifications encoded
        peptides (pd.DataFrame): DataFrame containing base peptide data with column:
            - "Sequence": Amino acid sequence of the peptide

    Returns:
        tuple containing:
            - peptide_id_map: Dictionary mapping peptide sequences to their modified peptide UUID
            - modified_peptides_batch: List of ModifiedPeptideDict records
            - modifications_batch: List of ModificationDict records
            - junction_batch: List of ModifiedPeptideModificationJunctionDict records

    Note:
        - Modifications marked as "Unmodified" are skipped
        - Uses deterministic UUIDs based on sequence and modifications
        - Deduplicates modified peptides and modifications
    """
    # Map from sequence to modified peptide UUID (for unmodified peptides)
    peptide_id_map: dict[str, uuid.UUID] = {}

    # Deduplication dicts
    modified_peptides_dict: dict[uuid.UUID, ModifiedPeptideDict] = {}
    modifications_dict: dict[uuid.UUID, ModificationDict] = {}
    junction_set: set[tuple[uuid.UUID, uuid.UUID]] = set()

    evidence = evidence.get(
        ["Sequence", "Modifications", "Modified sequence"], default=pd.DataFrame()
    )
    peptides = peptides.get(["Sequence"], default=pd.DataFrame())

    # Build map of sequence -> set of (modifications, modified_sequence) from evidence
    evidence_mod_map: dict[str, set[tuple[str, str]]] = {}
    for _, evidence_elem in evidence.iterrows():
        sequence = evidence_elem.get("Sequence", "")
        modifications: str = evidence_elem.get("Modifications", "")
        modified_sequence: str = evidence_elem.get("Modified sequence", "")
        if sequence:
            if sequence not in evidence_mod_map:
                evidence_mod_map[sequence] = set()
            evidence_mod_map[sequence].add((modifications, modified_sequence))

    # Process each peptide
    for _, peptide_elem in peptides.iterrows():
        sequence = peptide_elem.get("Sequence", "")
        if not sequence:
            continue

        evidences = evidence_mod_map.get(sequence, set())

        # Collect all unique modifications for this sequence
        all_mods: dict[str, list[tuple[int, str]]] = {}
        for modifications, modified_sequence in evidences:
            if modifications:
                if modifications == "Unmodified":
                    continue
                mod_list: list[str] = clean_mod_list_of_numbers(modifications.split(","))
                mods = extract_mods(modified_sequence, mod_list)

                for mod_name, mods_for_peptide in mods.items():
                    if mod_name not in all_mods:
                        all_mods[mod_name] = []
                    all_mods[mod_name].extend(
                        [m for m in mods_for_peptide if m not in all_mods[mod_name]]
                    )

        # Convert to ModificationDict format
        parsed_mods: list[ModificationDict] = []
        for mod_name, positions_residues in all_mods.items():
            for position, residue in positions_residues:
                unimod_id = lookup_unimod_id_by_name(mod_name)
                if unimod_id is None:
                    logger.warning(f"Could not find UNIMOD ID for modification: {mod_name}")
                    continue

                mod_uuid = generate_deterministic_modification_uuid(
                    unimod_id, None, position, residue
                )
                mod_dict: ModificationDict = {
                    "id": mod_uuid,
                    "unimod_id": unimod_id,
                    "name": None,
                    "location": position,
                    "modified_residue": residue,
                }
                parsed_mods.append(mod_dict)

        # Generate deterministic UUID from sequence and modifications
        modified_peptide_id = generate_deterministic_peptide_uuid(sequence, parsed_mods)

        # Create ModifiedPeptideDict
        modified_peptide_dict: ModifiedPeptideDict = {
            "id": modified_peptide_id,
            "peptide_sequence": sequence,
        }

        # Deduplicate: if same modified_peptide_id already exists, skip adding again
        if modified_peptide_id not in modified_peptides_dict:
            modified_peptides_dict[modified_peptide_id] = modified_peptide_dict

            # Add modifications and junctions
            for mod in parsed_mods:
                mod_id = mod["id"]
                modifications_dict[mod_id] = mod
                junction_set.add((modified_peptide_id, mod_id))

        # Map sequence to modified peptide UUID (used for PSM linking)
        peptide_id_map[sequence] = modified_peptide_id

    # Convert collections to lists for batch insertion
    modified_peptides_batch = list(modified_peptides_dict.values())
    modifications_batch = list(modifications_dict.values())
    junction_batch: list[ModifiedPeptideModificationJunctionDict] = [
        {"modified_peptide_id": mp_id, "modification_id": mod_id} for mp_id, mod_id in junction_set
    ]

    logger.debug(
        f"Created {len(modified_peptides_batch)} unique modified peptide records, "
        f"{len(modifications_batch)} unique modifications, and "
        f"{len(junction_batch)} unique junction entries"
    )
    return peptide_id_map, modified_peptides_batch, modifications_batch, junction_batch


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
            - peptide_evidence_batch: List of PeptideEvidenceDict records
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

    for _, peptide_row in peptides.iterrows():
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
        peptide_id_map (dict[str, uuid.UUID]): Mapping of peptide sequences to modified pep. UUIDs
        pe_id_map (dict[str, list[uuid.UUID]]): Mapping of peptide sequences to lists of peptide
            evidence UUIDs

    Returns:
        tuple containing:
            - psm_batch: List of PeptideSpectrumMatchDict records
            - junction_batch: List of PSMPeptideEvidenceDict junction records
            - search_mod_batch: List of SearchModificationDict records
    """
    psm_batch: list[PeptideSpectrumMatchDict] = []
    junction_batch: list[PSMPeptideEvidenceDict] = []
    search_mod_batch: list[SearchModificationDict] = []

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
    for _, summary_elem in summary.iterrows():
        raw_file: str = summary_elem.get("Raw file", "")
        var_modifications: str = summary_elem.get("Variable modifications", "")
        fixed_modified_sequence: str = summary_elem.get("Fixed modifications", "")
        if raw_file:
            if raw_file not in summary_mod_map:
                summary_mod_map[raw_file] = set()
            summary_mod_map[raw_file].add((var_modifications, fixed_modified_sequence))

    for _, psm_elem in evidence.iterrows():
        sequence: str = psm_elem.get("Sequence", "")
        modified_peptide_id: UUID | None = peptide_id_map.get(sequence)

        if not modified_peptide_id:
            logger.warning(f"Sequence '{sequence}' not found in peptide_id_map")
            continue

        scan_id: int = psm_elem.get("MS/MS scan number", "")
        mass = psm_elem.get("Mass", 0)
        charge = psm_elem.get("Charge", 1)

        modification_list: list[str] = []
        raw_file = psm_elem.get("Raw file", "")
        for var_modifications, fixed_modified_sequence in summary_mod_map.get(raw_file, set()):
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

        psm_id = uuid.uuid4()
        psm: PeptideSpectrumMatchDict = {
            "id": psm_id,
            "project_accession": project_accession,
            "mzid_file_id": None,  # txt.zip files don't have mzid_file_id
            "modified_peptide_id": modified_peptide_id,
            "spectrum_id": None,
            "charge_state": psm_elem.get("Charge", None),
            "experimental_mz": psm_elem.get("m/z", None),
            "calculated_mz": mass / charge if charge else None,
            "score_values": None,  # txt.zip files don't have score values in evidence.txt
            "rank": None,
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
    return psm_batch, junction_batch, search_mod_batch
