"""
Tests for parse_threshold_info() function
"""

from usigrabber.file_parser.mzid.parsing_functions import parse_threshold_info

# ============================================================================
# Basic parsing tests
# ============================================================================


def test_parse_threshold_info_with_real_fdr_file(fdr_reader):
    """Test parsing FDR threshold from minimal mzID file.

    Expected: threshold_type="pep:FDR threshold", threshold_value=0.01
    """
    threshold_type, threshold_value = parse_threshold_info(fdr_reader)

    assert threshold_type == "pep:FDR threshold"
    assert threshold_value == 0.01


def test_parse_threshold_info_with_real_mascot_file(mascot_reader):
    """Test parsing Mascot threshold from minimal mzID file.

    Expected: threshold_type="Mascot:SigThreshold", threshold_value=0.05
    """
    threshold_type, threshold_value = parse_threshold_info(mascot_reader)

    assert threshold_type == "Mascot:SigThreshold"
    assert threshold_value == 0.05


# ============================================================================
# Edge case tests
# ============================================================================


def test_parse_threshold_info_with_invalid_value(invalid_value_reader):
    """Test handling of invalid (non-numeric) threshold value.

    Expected: threshold_type="pep:FDR threshold", threshold_value=None
    """
    threshold_type, threshold_value = parse_threshold_info(invalid_value_reader)

    assert threshold_type == "pep:FDR threshold"
    assert threshold_value is None


def test_parse_threshold_info_with_empty_value(empty_value_reader):
    """Test handling of empty threshold value.

    Expected: threshold_type="pep:FDR threshold", threshold_value=None
    """
    threshold_type, threshold_value = parse_threshold_info(empty_value_reader)

    assert threshold_type == "pep:FDR threshold"
    assert threshold_value is None


def test_parse_threshold_info_with_no_threshold(no_threshold_reader):
    """Test handling of missing Threshold element.

    Expected: threshold_type=None, threshold_value=None
    """
    threshold_type, threshold_value = parse_threshold_info(no_threshold_reader)

    assert threshold_type is None
    assert threshold_value is None
