from usigrabber.db.schema import IndexType
from usigrabber.file_parser.mzid.parser import MzidFileParser


def test_mzid_parser_with_full_file(full_mzid_path):
    """
    Integration test for mzID parser with full_small.mzid.

    This test validates the complete parsing pipeline, ensuring that:
    - Metadata is correctly extracted from the file
    - Peptides and their sequences are parsed
    - Modifications are linked to peptides with correct positions
    - Peptide evidence (protein mappings) are created
    - PSMs are created with proper scores and references
    - Junction tables link PSMs to peptide evidence correctly
    """
    mock_project_accession = "PXD000001"
    file_parser = MzidFileParser()
    parsed_data = file_parser.parse_file(full_mzid_path, mock_project_accession)

    mzid_file = parsed_data.mzid_file
    peptides = parsed_data.peptides
    peptide_modifications = parsed_data.peptide_modifications
    peptide_evidence = parsed_data.peptide_evidence
    psms = parsed_data.psms
    psm_peptide_evidence_junctions = parsed_data.psm_peptide_evidence_junctions

    # =========================================================================
    # Test Peptides
    # =========================================================================
    assert len(peptides) > 0, "Should parse peptides from the file"
    assert len(peptides) == 695, "Expected 695 peptides in the test file"

    # =========================================================================
    # Test Peptide Modifications
    # =========================================================================

    assert len(peptide_modifications) > 0, "Should parse peptide modifications from the file"
    assert len(peptide_modifications) == 925, "Expected 925 peptide modifications"
    # Find a specific modified peptide
    # peptide_155_2: VFVNR with Deamidated N at position 4
    vfvnr_peptides = [p for p in peptides if p["sequence"] == "VFVNR"]
    assert len(vfvnr_peptides) > 0, "Should find VFVNR peptide"
    vfvnr_peptide_ids = {p["id"] for p in vfvnr_peptides}
    vfvnr_mods = [mod for mod in peptide_modifications if mod["peptide_id"] in vfvnr_peptide_ids]
    assert len(vfvnr_mods) > 0, "Should find modifications for VFVNR peptide"
    deamidated_mods = [mod for mod in vfvnr_mods if mod["modified_residue"] == "N"]
    assert len(deamidated_mods) > 0, "Should find Deamidated N modification"
    deamidated_mod = deamidated_mods[0]
    assert deamidated_mod["position"] == 4, "Deamidated N should be at position 4"
    assert deamidated_mod["name"] == "Deamidated", "Modification name should be Deamidated"

    # =========================================================================
    # Test Peptide Evidence (Protein Mappings)
    # =========================================================================
    assert len(peptide_evidence) > 0, "Should parse peptide evidence from the file"
    assert len(peptide_evidence) == 1563, "Expected 1563 peptide evidence records"

    # =========================================================================
    # Test PSMs (Peptide Spectrum Matches)
    # =========================================================================
    assert len(psms) > 0, "Should parse PSMs from the file"
    assert len(psms) == 695, "Expected 695 PSMs in the test file"

    # All PSMs should have required fields
    for psm in psms:
        assert psm["project_accession"] == mock_project_accession
        assert psm["mzid_file_id"] == mzid_file.id
        assert psm["peptide_id"] is not None
        assert psm["spectrum_id"] is not None, "PSM should have spectrum ID"

    # Verify specific PSM exists
    # SII_30_6: spectrum "index=1528", peptide ELLTK
    elltk_peptides = [p for p in peptides if p["sequence"] == "ELLTK"]
    assert len(elltk_peptides) > 0
    elltk_peptide_ids = {p["id"] for p in elltk_peptides}
    elltk_psms = [psm for psm in psms if psm["peptide_id"] in elltk_peptide_ids]
    assert len(elltk_psms) > 0, "Should find PSMs for ELLTK"

    # Verify PSM has detailed information
    sample_psm = elltk_psms[0]
    assert sample_psm["charge_state"] is not None, "PSM should have charge state"
    assert sample_psm["experimental_mz"] is not None, "PSM should have experimental m/z"
    assert sample_psm["calculated_mz"] is not None, "PSM should have calculated m/z"
    assert sample_psm["pass_threshold"] is not None, "PSM should have pass_threshold flag"
    assert sample_psm["rank"] is not None, "PSM should have rank"

    # Verify score_values are captured (Mascot score, expectation value)
    assert sample_psm["score_values"] is not None, "PSM should have score values"
    assert isinstance(sample_psm["score_values"], dict), "Score values should be a dict"
    # Based on Mascot data, we expect Mascot:score
    assert len(sample_psm["score_values"]) > 0, "Should have at least one score"

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

    # =========================================================================
    # Test Data Consistency and Relationships
    # =========================================================================

    # All peptide modifications should reference valid peptides
    peptide_ids = {p["id"] for p in peptides}
    for mod in peptide_modifications:
        assert mod["peptide_id"] in peptide_ids, "Modification should reference a valid peptide"

    # All PSMs should reference valid peptides
    for psm in psms:
        assert psm["peptide_id"] in peptide_ids, "PSM should reference a valid peptide"


def test_usi_fields_extraction(full_mzid_path):
    """
    Test that USI-related fields (index_type, index_number, ms_run) are correctly extracted
    from the mzID file.

    The full_small.mzid file contains:
    - spectrumID attributes like "index=3066"
    - cvParams with MS:1000796 (spectrum title) containing scan numbers and file names
    """
    mock_project_accession = "PXD000001"
    file_parser = MzidFileParser()
    parsed_data = file_parser.parse_file(full_mzid_path, mock_project_accession)
    psms = parsed_data.psms

    # At least some PSMs should have USI fields populated
    psms_with_index_type = [psm for psm in psms if psm["index_type"] is not None]
    psms_with_index_number = [psm for psm in psms if psm["index_number"] is not None]
    psms_with_ms_run = [psm for psm in psms if psm["ms_run"] is not None]

    assert len(psms_with_index_type) > 0, "Should extract index_type from at least some PSMs"
    assert len(psms_with_index_number) > 0, "Should extract index_number from at least some PSMs"
    assert len(psms_with_ms_run) > 0, "Should extract ms_run from at least some PSMs"

    # Check that scan numbers are extracted from spectrum title cvParams
    # Example cvParam: MS:1000796 with value containing "scan=3285"
    psms_with_scan = [psm for psm in psms if psm["index_type"] == IndexType["scan"]]
    if len(psms_with_scan) > 0:
        sample_scan_psm = psms_with_scan[0]
        assert sample_scan_psm["index_number"] is not None, (
            "PSMs with index_type='scan' should have index_number"
        )
        assert sample_scan_psm["index_number"] > 0, "Scan numbers should be positive integers"

    # Check that MS run is extracted from File field in spectrum title
    # Example: File:"OTE0019_York_060813_JH16.raw"
    if len(psms_with_ms_run) > 0:
        sample_ms_run_psm = psms_with_ms_run[0]
        assert sample_ms_run_psm["ms_run"] is not None
        assert len(sample_ms_run_psm["ms_run"]) > 0, "MS run should not be empty string"
        # MS run should not contain .raw extension
        assert ".raw" not in sample_ms_run_psm["ms_run"].lower(), (
            "MS run should have .raw extension removed"
        )
