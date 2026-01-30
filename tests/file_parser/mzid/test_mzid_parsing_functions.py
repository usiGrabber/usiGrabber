"""
Unit tests for individual mzID parsing functions.
"""

import os
from collections.abc import Generator
from pathlib import Path

import pytest

from usigrabber.db.schema import IndexType
from usigrabber.file_parser.mzid.parsing_functions import (
    parse_db_sequences,
    parse_mzid_metadata,
    parse_peptide_evidence,
    parse_peptides_and_modifications,
    parse_software_info,
    parse_spectra_data,
    parse_threshold_info,
)
from usigrabber.utils.file import temporary_path

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
# Tests for parse_peptides_and_modifications()
# ============================================================================


def test_parse_peptides_and_modifications(full_mzid_reader):
    """Test parsing peptides and modifications in a single pass."""
    peptide_id_map, peptides, modifications, junctions = parse_peptides_and_modifications(
        full_mzid_reader
    )

    # Test peptides
    assert len(peptides) > 0
    assert len(peptide_id_map) > 0
    sequences = {p["peptide_sequence"] for p in peptides}
    assert "ELLTK" in sequences
    assert "VFVNR" in sequences

    # Due to deduplication, peptides may be fewer than peptide_id_map
    # (same modified peptide can appear multiple times in mzID)
    assert len(peptides) <= len(peptide_id_map)

    # Test modifications
    assert len(modifications) >= 0  # May have no modifications in some files
    if len(modifications) > 0:
        # Check modification structure
        assert all("id" in mod for mod in modifications)
        assert all("unimod_id" in mod for mod in modifications)
        assert all("location" in mod for mod in modifications)
        assert all("modified_residue" in mod for mod in modifications)

    # Test junctions
    assert len(junctions) >= 0  # May have no junctions if no modifications
    if len(junctions) > 0:
        # Check junction structure
        assert all("modified_peptide_id" in j for j in junctions)
        assert all("modification_id" in j for j in junctions)


# ============================================================================
# Tests for parse_peptide_evidence()
# ============================================================================


def test_parse_peptide_evidence_basic(full_mzid_reader):
    """Test parsing peptide evidence from a real mzID file."""
    db_sequence_map = parse_db_sequences(full_mzid_reader)
    pe_id_map, peptide_evidence = parse_peptide_evidence(full_mzid_reader, db_sequence_map)

    assert len(peptide_evidence) > 0
    assert len(pe_id_map) > 0
    assert "P02768" in {pe["protein_accession"] for pe in peptide_evidence}
    assert "P01009" in {pe["protein_accession"] for pe in peptide_evidence}

    assert len(pe_id_map) == len(peptide_evidence)

    pe_with_positions = [pe for pe in peptide_evidence if pe["start_position"] is not None]
    assert len(pe_with_positions) > 0, "Some peptide evidence should have position info"

    for pe in pe_with_positions:
        if pe["start_position"] is not None:
            assert pe["start_position"] > 0, "Start position should be positive"
            if pe["end_position"] is not None:
                assert pe["end_position"] >= pe["start_position"], (
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
        assert pe["protein_accession"] is None


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


# ============================================================================
# Tests for Spectra Data
# ============================================================================


def test_parse_spectra_data(full_mzid_path) -> None:
    spectra_data = parse_spectra_data(full_mzid_path)

    assert len(spectra_data) == 1

    ms_run_name, spectrum_id_format = spectra_data["SD_1"]
    ms_run_name, ms_run_ext = os.path.splitext(ms_run_name)
    assert ms_run_name == "OTE0019_York_060813_JH16"
    assert ms_run_ext == ".mgf"
    assert spectrum_id_format == IndexType.index


@pytest.fixture(
    params=[
        "",  # some mzid files have no namespace
        "http://psidev.info/psi/pi/mzIdentML/1.1",  # test with different namespaces
        "http://psidev.info/psi/pi/mzIdentML/1.3",
    ]
)
def spectra_data_xml_generator(request) -> Generator[Path, None, None]:
    ns = request.param

    with temporary_path() as tmp_folder:
        temp_path = tmp_folder / "temp_mzid.xml"
        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(f"""
                <Inputs{' xmlns="' + ns + '"' if ns else ""}>
                    <SourceFile location="file:/E:/pp-2020-005%20Peaks%20Project/" id="PROJECT_1">
                    <FileFormat>
                        <cvParam accession="MS:1001107" cvRef="PSI-MS" name="data stored in database"/>
                    </FileFormat>
                    </SourceFile>
                    <SearchDatabase numDatabaseSequences="20274" location="file:/D:/Databases/uniprot_sprot%20(2).fasta" id="SEARCHDATABASE_1" name="UniProt_All">
                    <FileFormat>
                        <cvParam accession="MS:1001348" cvRef="PSI-MS" name="FASTA format"/>
                    </FileFormat>
                    <DatabaseName>
                        <userParam name="UniProt_All"/>
                    </DatabaseName>
                    </SearchDatabase>
                    <SpectraData location="file:/E:/vWA-related/pp-2020-005%20Peaks%20Project_PEAKS_56_HEK293_3/pp_2020_005_5_HEK293_3.mgf" id="SPECTRADATA_29">
                        <FileFormat>
                            <cvParam accession="MS:1001062" cvRef="PSI-MS" name="Mascot MGF file"/>
                        </FileFormat>
                        <SpectrumIDFormat>
                            <cvParam accession="MS:1000774" cvRef="PSI-MS" name="multiple peak list nativeID format"/>
                        </SpectrumIDFormat>
                    </SpectraData>
                    <SpectraData location="file:/E:/vWA-related/pp-2020-005%20Peaks%20Project_PEAKS_56_HEK293_3/pp_2020_005_5_HEK293_3.mgf" id="SPECTRADATA_30">
                        <FileFormat>
                            <cvParam accession="MS:1001062" cvRef="PSI-MS" name="Mascot MGF file"/>
                        </FileFormat>
                        <SpectrumIDFormat>
                            <cvParam accession="MS:1000774" cvRef="PSI-MS" name="multiple peak list nativeID format"/>
                        </SpectrumIDFormat>
                    </SpectraData>
                </Inputs>
                """)

        yield temp_path


def test_parse_spectra_data_custom_ns(spectra_data_xml_generator: Path) -> None:
    spectra_data = parse_spectra_data(spectra_data_xml_generator)

    assert len(spectra_data) == 2

    for ms_run_name, spectrum_id_format in spectra_data.values():
        ms_run_name, ms_run_ext = os.path.splitext(ms_run_name)
        assert ms_run_name == "pp_2020_005_5_HEK293_3"
        assert ms_run_ext == ".mgf"
        assert spectrum_id_format == IndexType.index
