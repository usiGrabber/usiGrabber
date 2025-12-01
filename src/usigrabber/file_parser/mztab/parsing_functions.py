import logging
import uuid

import pandas as pd
from pyteomics import mztab

logger = logging.getLogger(__name__)


# pyright: ignore[reportAttributeAccessIssue]
def extract_mztab_data(
    file: mztab.MzTab,
    project_accession: str,
) -> tuple[list[dict], list[dict]]:
    """
    Fast parser producing plain dict rows for bulk insert.
    """
    peptide_rows: list[dict] = []
    psm_rows: list[dict] = []

    pep_cache: dict[str, uuid.UUID] = {}  # sequence → peptide_uuid

    table = pd.DataFrame(file.spectrum_match_table)

    for row in table.itertuples(index=False):
        seq = row.sequence  # pyright: ignore[reportAttributeAccessIssue]

        # Deduplicate peptide sequences
        if seq not in pep_cache:
            pep_id = uuid.uuid4()
            pep_cache[seq] = pep_id
            peptide_rows.append(
                {
                    "id": pep_id,
                    "sequence": seq,
                    "length": len(seq),
                }
            )

        psm_rows.append(
            {
                "id": uuid.uuid4(),
                "project_accession": project_accession,
                "mzid_file_id": None,
                "peptide_id": pep_cache[seq],
                "spectrum_id": None,
                "charge_state": row.charge,  # pyright: ignore[reportAttributeAccessIssue]
                "experimental_mz": row.exp_mass_to_charge,  # pyright: ignore[reportAttributeAccessIssue]
                "calculated_mz": row.calc_mass_to_charge,  # pyright: ignore[reportAttributeAccessIssue]
                "score_values": None,
                "rank": None,
                "pass_threshold": None,
            }
        )

    return psm_rows, peptide_rows
