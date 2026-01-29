import logging
from collections.abc import Sequence
from pathlib import Path

import pytest
from sqlalchemy import Engine, func
from sqlmodel import Session, select

from usigrabber.db.schema import ImportedFile, MzidFile, PeptideSpectrumMatch
from usigrabber.file_parser import import_file

logger = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.slow
def test_import_mzid(engine: Engine, full_mzid_path: Path) -> None:
    """
    Integration test for importing an mzIdentML file.

    This test validates that:
    - The mzIdentML file is correctly imported into the database using
        the provided engine and project accession.
    - The MzidFile table contains exactly one entry with the expected
        file name and project accession.
    - The number of PeptideSpectrumMatch (PSM) records matches the
        count reported by the import statistics.
    """
    mock_project_accession = "PXD000010"

    import_file(engine, full_mzid_path, ".mzid", mock_project_accession)

    with Session(engine) as session:
        mzid_files: Sequence[MzidFile] = session.exec(select(MzidFile)).all()
        imported_files = session.exec(select(ImportedFile)).all()
        psm_count = session.exec(select(func.count()).select_from(PeptideSpectrumMatch)).one()

        assert len(imported_files) == 1
        assert len(mzid_files) == 1
        assert mzid_files[0].project_accession == mock_project_accession
        assert imported_files[0].checksum == "ee9e6cf94f58dcda5af2327a2f625346"
        assert psm_count == imported_files[0].psm_count
