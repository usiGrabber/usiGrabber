import logging
import uuid

import pandas as pd
from pyteomics import mztab

from usigrabber.file_parser.models import ModifiedPeptideDict, PeptideSpectrumMatchDict
from usigrabber.file_parser.uuid_helpers import generate_deterministic_peptide_uuid

logger = logging.getLogger(__name__)


# pyright: ignore[reportAttributeAccessIssue]
def extract_mztab_data(
    file: mztab.MzTab,
    project_accession: str,
) -> tuple[list[PeptideSpectrumMatchDict], list[ModifiedPeptideDict]]:
    """
    Fast parser producing plain dict rows for bulk insert.
    """
    unique_modified_peptides: dict[str, ModifiedPeptideDict] = {}
    psm_rows: list[PeptideSpectrumMatchDict] = []

    table = pd.DataFrame(file.spectrum_match_table)

    for row in table.itertuples(index=False):
        seq = row.sequence  # pyright: ignore[reportAttributeAccessIssue]

        # Deduplicated based on sequence only for mzTab. TODO: consider modifications
        peptide_id = generate_deterministic_peptide_uuid(seq, "")
        peptide_dict: ModifiedPeptideDict = {"id": peptide_id, "peptide_sequence": seq}
        unique_modified_peptides[seq] = peptide_dict

        psm_dict: PeptideSpectrumMatchDict = {
            "id": uuid.uuid4(),
            "project_accession": project_accession,
            "mzid_file_id": None,
            "modified_peptide_id": unique_modified_peptides[seq]["id"],
            "spectrum_id": None,
            "charge_state": row.charge,  # pyright: ignore[reportAttributeAccessIssue]
            "experimental_mz": row.exp_mass_to_charge,  # pyright: ignore[reportAttributeAccessIssue]
            "calculated_mz": row.calc_mass_to_charge,  # pyright: ignore[reportAttributeAccessIssue]
            "score_values": None,
            "rank": None,
            "pass_threshold": None,
            "index_type": None,
            "index_number": None,
            "ms_run": None,
        }
        psm_rows.append(psm_dict)

    modified_peptide_rows = list(unique_modified_peptides.values())

    return psm_rows, modified_peptide_rows
