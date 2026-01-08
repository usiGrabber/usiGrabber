from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from usigrabber.db.schema import IndexType, ModifiedPeptide, PeptideSpectrumMatch, Project
from usigrabber.usi_validation.validator import validate_psms_batch


@pytest.fixture
def sample_psm():
    project = Project(accession="PXD000001", title="Test", submissionType="COMPLETE")
    modified_peptide = ModifiedPeptide(peptide_sequence="PEPTIDE", id=uuid4())
    return PeptideSpectrumMatch(
        id=uuid4(),
        project=project,
        project_accession=project.accession,
        ms_run="run1",
        index_type=IndexType.scan,
        index_number=123,
        modified_peptide=modified_peptide,
        modified_peptide_id=modified_peptide.id,
        charge_state=2,
        spectrum_id="spec1",
        experimental_mz=100.0,
        calculated_mz=100.0,
        pass_threshold=True,
    )


def test_validate_psms_batch_success(sample_psm):
    backend_mock = MagicMock()
    backend_mock.validate_usi.return_value = True

    psms = [sample_psm]
    # Pass backend_mock as Any to avoid type errors in tests
    results = validate_psms_batch(psms, backend_mock)  # type: ignore

    assert len(results) == 1
    assert results[sample_psm.id] is True
    backend_mock.validate_usi.assert_called_once()


def test_validate_psms_batch_invalid_usi(sample_psm):
    backend_mock = MagicMock()
    backend_mock.validate_usi.return_value = False

    psms = [sample_psm]
    results = validate_psms_batch(psms, backend_mock)  # type: ignore

    assert len(results) == 1
    assert results[sample_psm.id] is False


def test_validate_psms_batch_missing_fields():
    # PSM with missing project
    psm = PeptideSpectrumMatch(
        id=uuid4(),
        ms_run="run1",
        project_accession="PXD1",
        modified_peptide_id=uuid4(),
        spectrum_id="s1",
        charge_state=2,
        experimental_mz=1,
        calculated_mz=1,
        pass_threshold=True,
    )
    backend_mock = MagicMock()

    results = validate_psms_batch([psm], backend_mock)  # type: ignore

    assert len(results) == 1
    assert results[psm.id] is False
    backend_mock.validate_usi.assert_not_called()


@patch("time.sleep", return_value=None)
@patch("time.time")
def test_validate_psms_batch_rate_limiting(mock_time, mock_sleep, sample_psm):
    backend_mock = MagicMock()
    backend_mock.validate_usi.return_value = True

    # Mock time to always return 0, then 0.1, 0.2...
    mock_time.side_effect = [i * 0.01 for i in range(1000)]

    psms = [sample_psm] * 5
    # Limit to 2 requests per second
    validate_psms_batch(psms, backend_mock, requests_per_second=2.0)  # type: ignore

    # Should have called sleep
    assert mock_sleep.called
