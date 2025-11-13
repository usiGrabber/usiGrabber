"""
Tests for mzID helper functions

Unit tests for pure parsing helper functions with no database dependencies.
Tests edge cases, error handling, and data transformations.
"""

from usigrabber.file_parser.mzid.helpers import (
    extract_score_values,
    extract_unimod_id,
    parse_modification_location,
)

# ============================================================================
# Tests for extract_unimod_id()
# ============================================================================


def test_extract_unimod_id_from_single_cvparam():
    """Test extracting UNIMOD ID from single cvParam with accession."""
    mod_data = {"cvParam": {"accession": "UNIMOD:30", "name": ""}}

    result = extract_unimod_id(mod_data)

    assert result == 30


def test_extract_unimod_id_from_cvparam_list():
    """Test extracting UNIMOD ID from cvParam list."""
    mod_data = {
        "cvParam": [
            {"accession": "MS:1001524", "name": "fragment neutral loss"},
            {"accession": "UNIMOD:34", "name": ""},
        ]
    }

    result = extract_unimod_id(mod_data)

    assert result == 34


def test_extract_unimod_id_with_invalid_accession():
    """Test handling of invalid UNIMOD accession format (too short)."""
    mod_data = {"cvParam": {"accession": "UNIMOD:", "name": "Oxidation"}}
    result = extract_unimod_id(mod_data)
    assert result == 35  # Falls back to name-based resolution


def test_extract_unimod_id_with_non_numeric_accession():
    """Test handling of non-numeric UNIMOD accession."""
    mod_data = {"cvParam": {"accession": "UNIMOD:ABC", "name": "Oxidation"}}

    result = extract_unimod_id(mod_data)

    assert result == 35


def test_extract_unimod_id_empty_mod_data():
    """Test handling empty modification data."""
    mod_data = {}

    result = extract_unimod_id(mod_data)

    assert result is None


# ============================================================================
# Tests for extract_score_values()
# ============================================================================


def test_extract_score_values_basic():
    """Test extracting basic score values."""
    sii = {
        "Mascot:score": 45.2,
        "MS:1002252": 0.001,  # MS-GF:SpecEValue
        "chargeState": 2,
        "experimentalMassToCharge": 500.25,
    }

    result = extract_score_values(sii)

    assert "Mascot:score" in result
    assert result["Mascot:score"] == 45.2
    # MS:1002252 doesn't match pattern, so not included
    assert "MS:1002252" not in result


def test_extract_score_values_no_scores():
    """Test handling when no score fields are present."""
    sii = {
        "chargeState": 2,
        "experimentalMassToCharge": 500.25,
        "peptide_ref": "PEP_123",
    }

    result = extract_score_values(sii)

    assert result == {}


# ============================================================================
# Tests for parse_modification_location()
# ============================================================================


def test_parse_modification_location_basic():
    """Test parsing modification with basic location and residue."""
    mod = {"location": 5, "residues": "M"}

    location, residues = parse_modification_location(mod)

    assert location == 5
    assert residues == "M"


def test_parse_modification_location_residues_as_list():
    """Test parsing modification with residues as list."""
    mod = {"location": 3, "residues": ["S", "T"]}

    location, residues = parse_modification_location(mod)

    assert location == 3
    assert residues == "ST"


def test_parse_modification_location_missing_fields():
    """Test parsing modification with missing location or residues."""
    mod = {"location": 5}

    location, residues = parse_modification_location(mod)

    assert location == 5
    assert residues is None


def test_parse_modification_location_empty_dict():
    """Test parsing empty modification dictionary."""
    mod = {}

    location, residues = parse_modification_location(mod)

    assert location is None
    assert residues is None
