"""
Unit tests for individual mzID parsing functions.
"""

import uuid

from usigrabber.file_parser.mzid.parsing_functions import (
    link_modifications,
    parse_db_sequences,
    parse_mzid_metadata,
    parse_peptide_evidence,
    parse_peptides,
    parse_software_info,
    parse_threshold_info,
)

# ============================================================================
# Tests for parse_software_info()
# ============================================================================


def test_parse_software_info_basic(full_mzid_reader):
    """Test parsing software info from a real mzID file."""
    software_name, software_version = parse_software_info(full_mzid_reader)

    assert software_name is not None
    assert software_version is not None


def test_parse_software_info_returns_first_only(full_mzid_reader):
    """Test that only the first software is returned."""
    software_name, software_version = parse_software_info(full_mzid_reader)

    assert software_name == "Mascot Server"
    assert software_version == "2.4.1"


# ============================================================================
# Tests for parse_db_sequences()
# ============================================================================


def test_parse_db_sequences(full_mzid_reader):
    """Test parsing DB sequences from a real mzID file."""
    db_sequence_map = parse_db_sequences(full_mzid_reader)

    assert len(db_sequence_map) > 0
    accessions = set(db_sequence_map.values())
    assert "P02768" in accessions  # Serum albumin
    assert "P01009" in accessions  # Alpha-1-antitrypsin


# ============================================================================
# Tests for parse_peptides()
# ============================================================================


def test_parse_peptides(full_mzid_reader):
    """Test parsing peptides from a real mzID file."""
    peptide_id_map, peptide_mods, peptides = parse_peptides(full_mzid_reader)

    assert len(peptides) > 0
    assert len(peptide_id_map) > 0
    assert len(peptide_mods) > 0
    sequences = {p.sequence for p in peptides}
    assert "ELLTK" in sequences
    assert "VFVNR" in sequences

    assert len(peptide_id_map) == len(peptides)


# ============================================================================
# Tests for parse_peptide_evidence()
# ============================================================================


def test_parse_peptide_evidence_basic(full_mzid_reader):
    """Test parsing peptide evidence from a real mzID file."""
    db_sequence_map = parse_db_sequences(full_mzid_reader)
    pe_id_map, peptide_evidence = parse_peptide_evidence(full_mzid_reader, db_sequence_map)

    assert len(peptide_evidence) > 0
    assert len(pe_id_map) > 0
    assert "P02768" in {pe.protein_accession for pe in peptide_evidence}
    assert "P01009" in {pe.protein_accession for pe in peptide_evidence}

    assert len(pe_id_map) == len(peptide_evidence)

    pe_with_positions = [pe for pe in peptide_evidence if pe.start_position is not None]
    assert len(pe_with_positions) > 0, "Some peptide evidence should have position info"

    for pe in pe_with_positions:
        if pe.start_position is not None:
            assert pe.start_position > 0, "Start position should be positive"
            if pe.end_position is not None:
                assert pe.end_position >= pe.start_position, (
                    "End position should be >= start position"
                )


def test_parse_peptide_evidence_with_empty_db_map(full_mzid_reader):
    """Test handling when db_sequence_map is empty."""
    empty_map = {}
    pe_id_map, peptide_evidence = parse_peptide_evidence(full_mzid_reader, empty_map)

    assert len(peptide_evidence) > 0
    assert len(pe_id_map) > 0
    assert len(pe_id_map) == len(peptide_evidence)

    for pe in peptide_evidence:
        assert pe.protein_accession is None


# ============================================================================
# Tests for link_modifications()
# ============================================================================


def test_link_modifications():
    """Test linking modifications to peptides."""
    peptide1 = uuid.uuid4()
    peptide2 = uuid.uuid4()
    mock_peptide_mods = {
        peptide1: [
            {"location": 3, "residues": "M", "unimod_id": 35, "name": "Oxidation"},
            {"location": 7, "residues": "K", "unimod_id": 1, "name": "Acetylation"},
        ],
        peptide2: [
            {"location": 1, "residues": ["N"], "unimod_id": 21, "name": "Phospho"},
        ],
    }
    mod_batch = link_modifications(mock_peptide_mods)
    assert len(mod_batch) == 3

    mods_uuid1 = [mod for mod in mod_batch if mod.peptide_id == peptide1]
    assert len(mods_uuid1) == 2
    assert any(
        mod.position == 3 and mod.unimod_id == 35 and mod.name == "Oxidation" for mod in mods_uuid1
    )

    peptide2_mod = mod_batch[[mod.peptide_id for mod in mod_batch].index(peptide2)]
    assert peptide2_mod.position == 1
    assert peptide2_mod.unimod_id == 21
    assert peptide2_mod.name == "Phospho"
    assert peptide2_mod.modified_residue == "N"


# ============================================================================
# Tests for parse_mzid_metadata()
# ============================================================================


def test_parse_mzid_metadata_basic(full_mzid_reader, full_mzid_path):
    """Test parsing mzID metadata from a real file."""
    project_accession = "PXD000001"

    mzid_file = parse_mzid_metadata(full_mzid_reader, full_mzid_path, project_accession)

    # Check basic fields
    assert mzid_file.project_accession == project_accession
    assert mzid_file.software_name == "Mascot Server"
    assert mzid_file.software_version == "2.4.1"
    assert mzid_file.threshold_type == "Mascot:SigThreshold"
    assert mzid_file.threshold_value == 0.05
    assert mzid_file.creation_date is not None


# ===========================================================================
# Tests for parse_threshold_info()
# ===========================================================================


def test_parse_threshold_info_with_fdr_file(fdr_reader):
    """Test parsing FDR threshold from minimal mzID file.

    Expected: threshold_type="pep:FDR threshold", threshold_value=0.01
    """
    threshold_type, threshold_value = parse_threshold_info(fdr_reader)

    assert threshold_type == "pep:FDR threshold"
    assert threshold_value == 0.01


def test_parse_threshold_info_with_real_file(mascot_reader):
    """Test parsing Mascot threshold from minimal mzID file.

    Expected: threshold_type="Mascot:SigThreshold", threshold_value=0.05
    """
    threshold_type, threshold_value = parse_threshold_info(mascot_reader)

    assert threshold_type == "Mascot:SigThreshold"
    assert threshold_value == 0.05


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
