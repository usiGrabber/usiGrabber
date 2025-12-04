import logging
from collections.abc import Sequence
from pathlib import Path

import pytest
from sqlalchemy import Engine
from sqlmodel import Session, func, select

from usigrabber.db.schema import MzidFile, PeptideSpectrumMatch
from usigrabber.file_parser import import_file

logger = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.slow
def test_import_mzid(engine: Engine, full_mzid_path: Path) -> None:
    mock_project_accession = "PXD000010"

    import_stats = import_file(engine, full_mzid_path, mock_project_accession)

    with Session(engine) as session:
        mzid_files: Sequence[MzidFile] = session.exec(select(MzidFile)).all()
        assert len(mzid_files) == 1
        assert mzid_files[0].file_name == full_mzid_path.name == import_stats.file_name
        assert mzid_files[0].project_accession == mock_project_accession

        psm_count = session.exec(select(func.count()).select_from(PeptideSpectrumMatch)).one()
        assert psm_count == import_stats.psm_count
