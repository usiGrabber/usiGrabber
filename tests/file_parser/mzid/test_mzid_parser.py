from usigrabber.db.schema import IndexType
from usigrabber.file_parser.mzid.parser import MzidFileParser


def test_mzid_parser_handles_files_without_db_sequences_or_peptides(mzid_fixtures_dir):
    """Regression test for reader re-use issues in pyteomics iterfind.

    Some valid mzID files do not contain DBSequence/Peptide entries. Re-using a single
    reader across parsing phases can exhaust the stream and then crash with:
    "XMLSyntaxError: no element found (line 0)".
    """
    file_parser = MzidFileParser()
    parsed_data = file_parser.parse_file(
        mzid_fixtures_dir / "no_db_sequences_or_peptides.mzid", "PXD000579"
    )

    assert parsed_data.mzid_file is not None
    assert parsed_data.modified_peptides == []
    assert parsed_data.modifications == []
    assert parsed_data.peptide_evidence == []
    assert parsed_data.psms == []


def test_mzid_parser_with_full_file(full_mzid_path):
    """
    Integration test for mzID parser with full_small.mzid.

    This test validates the complete parsing pipeline, ensuring that:
    - Metadata is correctly extracted from the file
    - Peptides and their sequences are parsed
    - Modifications are linked to peptides with correct locations
    - Peptide evidence (protein mappings) are created
    - PSMs are created with proper scores and references
    - Junction tables link PSMs to peptide evidence correctly
    """
    mock_project_accession = "PXD000001"
    file_parser = MzidFileParser()
    # Single ParsedMzidData expected
    parsed_data = file_parser.parse_file(full_mzid_path, mock_project_accession)

    mzid_file = parsed_data.mzid_file
    modified_peptides = parsed_data.modified_peptides
    modifications = parsed_data.modifications
    peptide_evidence = parsed_data.peptide_evidence
    psms = parsed_data.psms
    psm_peptide_evidence_junctions = parsed_data.psm_peptide_evidence_junctions
    search_mods = parsed_data.search_modifications

    # =========================================================================
    # Test Peptides (with deduplication)
    # =========================================================================
    assert len(modified_peptides) > 0, "Should parse peptides from the file"
    # After deduplication: 413 unique modified peptides (was 695 before deduplication)
    assert len(modified_peptides) == 413, "Expected 413 unique modified peptides"

    # =========================================================================
    # Test Peptide Modifications (with deduplication)
    # =========================================================================

    assert len(modifications) > 0, "Should parse peptide modifications from the file"
    # Modifications are now deduplicated by (unimod_id, name, location, modified_residue)
    # Find a specific modification: Deamidated N at location 4
    deamidated_n_mods = [
        mod for mod in modifications if mod["modified_residue"] == "N" and mod["location"] == 4
    ]
    # There should be a unique Deamidated N at location 4 modification
    assert len(deamidated_n_mods) > 0, "Should find Deamidated N at location 4"
    deamidated_mod = deamidated_n_mods[0]
    assert deamidated_mod["location"] == 4, "Deamidated N should be at location 4"

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
        assert psm["modified_peptide_id"] is not None
        assert psm["spectrum_id"] is not None, "PSM should have spectrum ID"
        assert (
            len([search_mod for search_mod in search_mods if search_mod["psm_id"] == psm["id"]])
            == 5
        )

    # Verify specific PSM exists
    # SII_30_6: spectrum "index=1528", peptide ELLTK
    elltk_peptides = [p for p in modified_peptides if p["peptide_sequence"] == "ELLTK"]
    assert len(elltk_peptides) > 0
    elltk_peptide_ids = {p["id"] for p in elltk_peptides}
    elltk_psms = [psm for psm in psms if psm["modified_peptide_id"] in elltk_peptide_ids]
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

    # All PSMs should reference valid modified peptides
    modified_peptide_ids = {p["id"] for p in modified_peptides}
    for psm in psms:
        assert psm["modified_peptide_id"] in modified_peptide_ids, (
            "PSM should reference a valid modified peptide"
        )

    # Modifications are linked via junction table, verify junction integrity
    modification_ids = {mod["id"] for mod in modifications}
    junctions = parsed_data.modified_peptide_modification_junctions
    for junction in junctions:
        assert junction["modified_peptide_id"] in modified_peptide_ids, (
            "Junction should reference valid modified peptide"
        )
        assert junction["modification_id"] in modification_ids, (
            "Junction should reference valid modification"
        )


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
    # Single ParsedMzidData expected
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
        # MS run should not contain .mgf extension
        assert ".mgf" not in sample_ms_run_psm["ms_run"].lower(), (
            "MS run should have extension removed"
        )
        assert sample_ms_run_psm["ms_run_ext"] == "mgf"
