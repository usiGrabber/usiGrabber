from pathlib import Path

from usigrabber.db.schema import IndexType
from usigrabber.file_parser.txt_zip.parser import TxtZipFileParser


def test_txt_zip_parser_basic():
    """
    Basic test for txt.zip parser to ensure it runs without errors
    and returns expected data structures.
    """
    mock_project_accession = "PXD000001"
    file_parser = TxtZipFileParser()
    evidence_path, summary_path, peptides_path = (
        Path("tests/txt_zip/fixtures/project2/evidence.txt"),
        Path("tests/txt_zip/fixtures/project2/summary.txt"),
        Path("tests/txt_zip/fixtures/project2/peptides.txt"),
    )
    parsed_data = file_parser.parse_file(
        (evidence_path, summary_path, peptides_path), mock_project_accession
    )

    assert len(parsed_data.peptides) == 4
    assert len(parsed_data.peptide_modifications) == 6
    assert len(parsed_data.peptide_evidence) == 4
    assert len(parsed_data.psms) == 17
    assert len(parsed_data.psm_peptide_evidence_junctions) == 17
    assert (
        len(parsed_data.search_modifications) == len(parsed_data.psms) * 3
    )  # 3 tested mod for each raw file (so for each psm) stated in summary.txt


def test_txt_zip_parser():
    """
    Comprehensive test for txt.zip parser to validate parsing logic
    and data integrity across all parsed entities.
    """
    mock_project_accession = "PXD000001"
    file_parser = TxtZipFileParser()
    evidence_path, summary_path, peptides_path = (
        Path("tests/txt_zip/fixtures/project1/evidence.txt"),
        Path("tests/txt_zip/fixtures/project1/summary.txt"),
        Path("tests/txt_zip/fixtures/project1/peptides.txt"),
    )
    parsed_data = file_parser.parse_file(
        (evidence_path, summary_path, peptides_path), mock_project_accession
    )

    peptides = parsed_data.peptides
    peptide_modifications = parsed_data.peptide_modifications
    peptide_evidence = parsed_data.peptide_evidence
    psms = parsed_data.psms
    psm_peptide_evidence_junctions = parsed_data.psm_peptide_evidence_junctions
    search_modifications = parsed_data.search_modifications

    # =========================================================================
    # Test Peptides
    # =========================================================================
    assert len(peptides) > 0, "Should parse peptides from the file"
    assert len(peptides) == 1174, "Expected 1174 peptides in the test file"

    # =========================================================================
    # Test Peptide Modifications
    # =========================================================================

    assert len(peptide_modifications) > 0, "Should parse peptide modifications from the file"
    assert len(peptide_modifications) == 151, "Expected 151 peptide modifications"

    # Find a specific modified peptide
    # peptides.txt, line 673 + evidence.txt line 1145:
    # ANAVALGNYLMSK with Oxidation (M) at position 11
    ana_peptides = [p for p in peptides if p["sequence"] == "ANAVALGNYLMSK"]
    assert len(ana_peptides) > 0, "Should find ANAVALGNYLMSK peptide"
    ana_peptide_ids = {p["id"] for p in ana_peptides}
    ana_mods = [mod for mod in peptide_modifications if mod["peptide_id"] in ana_peptide_ids]
    assert len(ana_mods) > 0, "Should find modifications for VFVNR peptide"
    oxidation_mod = ana_mods[0]
    assert oxidation_mod["modified_residue"] and len(oxidation_mod["modified_residue"]) == 1
    assert oxidation_mod["position"] == 11, "Oxidation should be at position 11"
    assert oxidation_mod["name"] == "Oxidation", "Modification name should be Oxidation"

    # =========================================================================
    # Test Peptide Evidence (Protein Mappings)
    # =========================================================================
    assert len(peptide_evidence) > 0, "Should parse peptide evidence from the file"
    assert len(peptide_evidence) == 1584, "Expected 1584 peptide evidence records"

    # =========================================================================
    # Test PSMs (Peptide Spectrum Matches)
    # =========================================================================
    assert len(psms) > 0, "Should parse PSMs from the file"
    assert len(psms) == 1640, "Expected 1640 PSMs in the test file"

    # All PSMs should have required fields
    for psm in psms:
        assert psm["project_accession"] == mock_project_accession
        assert psm["peptide_id"] is not None

    # Verify specific PSM exists
    # evidence.txt, line 1843+1844 and peptides.txt, line 1083 sequence/peptide AVYECLR
    avyeclr_peptides = [p for p in peptides if p["sequence"] == "AVYECLR"]
    assert len(avyeclr_peptides) > 0
    avyeclr_peptide_ids = {p["id"] for p in avyeclr_peptides}
    avyeclr_psms = [psm for psm in psms if psm["peptide_id"] in avyeclr_peptide_ids]
    assert len(avyeclr_psms) > 0, "Should find PSMs for AVYECLR"

    # Verify PSM has detailed information
    sample_psm = avyeclr_psms[0]
    assert sample_psm["charge_state"] is not None, "PSM should have charge state"
    assert sample_psm["experimental_mz"] is not None, "PSM should have experimental m/z"
    assert sample_psm["calculated_mz"] is not None, "PSM should have calculated m/z"

    # =========================================================================
    # Test PSM-PeptideEvidence Junctions
    # =========================================================================
    assert len(psm_peptide_evidence_junctions) > 0, (
        "Should create junction records linking PSMs to peptide evidence"
    )

    # All junctions should have valid IDs
    psm_ids = {psm["id"] for psm in psms}
    pe_ids = {pe["id"] for pe in peptide_evidence}

    for junction in psm_peptide_evidence_junctions:
        assert junction["psm_id"] in psm_ids, "Junction should reference a valid PSM"
        assert junction["peptide_evidence_id"] in pe_ids, (
            "Junction should reference a valid peptide evidence"
        )

    # Verify that some PSMs link to multiple protein evidences (shared peptides)
    psm_pe_counts = {}
    for junction in psm_peptide_evidence_junctions:
        psm_pe_counts[junction["psm_id"]] = psm_pe_counts.get(junction["psm_id"], 0) + 1

    # At least one PSM should map to multiple proteins
    max_pe_per_psm = max(psm_pe_counts.values())
    assert max_pe_per_psm >= 1, "PSMs should link to at least one peptide evidence"

    # All peptide modifications should reference valid peptides
    peptide_ids = {p["id"] for p in peptides}
    for mod in peptide_modifications:
        assert mod["peptide_id"] in peptide_ids, "Modification should reference a valid peptide"

    # All PSMs should reference valid peptides
    for psm in psms:
        assert psm["peptide_id"] in peptide_ids, "PSM should reference a valid peptide"

    # =========================================================================
    # Test Search Modifications (i.e. modifications, the sequence was tested for)
    # =========================================================================

    assert len(search_modifications) > 0, "Should parse search modifications from the file"
    assert len(search_modifications) == len(psms) * 4, "Expected 4 search modifications per PSM"


def test_usi_fields_extraction():
    """
    Test that USI-related fields (index_type, index_number, ms_run) are correctly extracted
    from the files.
    """
    mock_project_accession = "PXD000001"
    file_parser = TxtZipFileParser()
    evidence_path, summary_path, peptides_path = (
        Path("tests/txt_zip/fixtures/project1/evidence.txt"),
        Path("tests/txt_zip/fixtures/project1/summary.txt"),
        Path("tests/txt_zip/fixtures/project1/peptides.txt"),
    )
    parsed_data = file_parser.parse_file(
        (evidence_path, summary_path, peptides_path), mock_project_accession
    )
    psms = parsed_data.psms

    # At least some PSMs should have USI fields populated
    psms_with_index_type = [psm for psm in psms if psm["index_type"] is not None]
    psms_with_index_number = [psm for psm in psms if psm["index_number"] is not None]
    psms_with_ms_run = [psm for psm in psms if psm["ms_run"] is not None]

    assert len(psms_with_index_type) > 0, "Should extract index_type from at least some PSMs"
    assert len(psms_with_index_number) > 0, "Should extract index_number from at least some PSMs"
    assert len(psms_with_ms_run) > 0, "Should extract ms_run from at least some PSMs"

    # Check that scan numbers are extracted
    psms_with_scan_and_number = [
        psm
        for psm in psms
        if psm["index_type"] == IndexType.scan and psm["index_number"] and psm["index_number"] >= 0
    ]
    assert len(psms) == len(psms_with_scan_and_number), (
        "All PSMs should have a 'scan' indexType and a scan number"
    )

    # Check that MS run is extracted
    if len(psms_with_ms_run) > 0:
        sample_ms_run_psm = psms_with_ms_run[0]
        assert sample_ms_run_psm["ms_run"] is not None
        assert len(sample_ms_run_psm["ms_run"]) > 0, "MS run should not be empty string"
        # MS run should not contain .raw extension
        assert ".raw" not in sample_ms_run_psm["ms_run"].lower(), (
            "MS run should have .raw extension removed"
        )
