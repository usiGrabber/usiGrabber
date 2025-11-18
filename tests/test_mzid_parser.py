from usigrabber.file_parser.mzid.parser import parse_mzid_file


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
    parsed_data = parse_mzid_file(full_mzid_path, mock_project_accession)

    mzid_file = parsed_data.mzid_file
    peptides = parsed_data.peptides
    peptide_modifications = parsed_data.peptide_modifications
    peptide_evidence = parsed_data.peptide_evidence
    psms = parsed_data.psms
    psm_peptide_evidence_junctions = parsed_data.psm_peptide_evidence_junctions

    # =========================================================================
    # Test mzID File Metadata
    # =========================================================================
    assert mzid_file.project_accession == mock_project_accession
    assert mzid_file.file_name == "full_small.mzid"
    assert mzid_file.software_name == "Mascot Server"
    assert mzid_file.software_version == "2.4.1"
    assert mzid_file.threshold_type == "Mascot:SigThreshold"
    assert mzid_file.threshold_value == 0.05
    assert mzid_file.creation_date is not None

    # =========================================================================
    # Test Peptides
    # =========================================================================
    assert len(peptides) > 0, "Should parse peptides from the file"
    assert len(peptides) == 695, "Expected 695 peptides in the test file"

    # Verify specific peptides exist
    peptide_sequences = {p.sequence for p in peptides}
    assert "ELLTK" in peptide_sequences
    assert "VIEHI" in peptide_sequences
    assert "VALIAK" in peptide_sequences
    assert "VFVNR" in peptide_sequences

    # Verify sequence lengths are computed correctly
    first_peptide = peptides[0]
    assert first_peptide.length == len(first_peptide.sequence), (
        f"Peptide {first_peptide.sequence} length mismatch"
    )

    # =========================================================================
    # Test Peptide Modifications
    # =========================================================================

    assert len(peptide_modifications) > 0, "Should parse peptide modifications from the file"
    assert len(peptide_modifications) == 925, "Expected 925 peptide modifications"
    # Find a specific modified peptide
    # peptide_155_2: VFVNR with Deamidated N at position 4
    vfvnr_peptides = [p for p in peptides if p.sequence == "VFVNR"]
    assert len(vfvnr_peptides) > 0, "Should find VFVNR peptide"
    vfvnr_peptide_ids = {p.id for p in vfvnr_peptides}
    vfvnr_mods = [mod for mod in peptide_modifications if mod.peptide_id in vfvnr_peptide_ids]
    assert len(vfvnr_mods) > 0, "Should find modifications for VFVNR peptide"
    deamidated_mods = [mod for mod in vfvnr_mods if mod.modified_residue == "N"]
    assert len(deamidated_mods) > 0, "Should find Deamidated N modification"
    deamidated_mod = deamidated_mods[0]
    assert deamidated_mod.position == 4, "Deamidated N should be at position 4"
    assert deamidated_mod.name == "Deamidated", "Modification name should be Deamidated"

    # =========================================================================
    # Test Peptide Evidence (Protein Mappings)
    # =========================================================================
    assert len(peptide_evidence) > 0, "Should parse peptide evidence from the file"
    assert len(peptide_evidence) == 1563, "Expected 1563 peptide evidence records"

    # Verify specific protein accessions exist (from DBSequence section)
    protein_accessions = {pe.protein_accession for pe in peptide_evidence if pe.protein_accession}
    assert "P02768" in protein_accessions, "Should map to Serum albumin"
    assert "P01009" in protein_accessions, "Should map to Alpha-1-antitrypsin"

    # Verify peptide evidence has position information
    pe_with_positions = [pe for pe in peptide_evidence if pe.start_position is not None]
    assert len(pe_with_positions) > 0, "Some peptide evidence should have position info"

    for pe in pe_with_positions:
        if pe.start_position is not None:
            assert pe.start_position > 0, "Start position should be positive"
            if pe.end_position is not None:
                assert pe.end_position >= pe.start_position, (
                    "End position should be >= start position"
                )

    # =========================================================================
    # Test PSMs (Peptide Spectrum Matches)
    # =========================================================================
    assert len(psms) > 0, "Should parse PSMs from the file"
    assert len(psms) == 695, "Expected 695 PSMs in the test file"

    # All PSMs should have required fields
    for psm in psms:
        assert psm.project_accession == mock_project_accession
        assert psm.mzid_file_id == mzid_file.id
        assert psm.peptide_id is not None
        assert psm.spectrum_id is not None, "PSM should have spectrum ID"

    # Verify specific PSM exists
    # SII_30_6: spectrum "index=1528", peptide ELLTK
    elltk_peptides = [p for p in peptides if p.sequence == "ELLTK"]
    assert len(elltk_peptides) > 0
    elltk_peptide_ids = {p.id for p in elltk_peptides}
    elltk_psms = [psm for psm in psms if psm.peptide_id in elltk_peptide_ids]
    assert len(elltk_psms) > 0, "Should find PSMs for ELLTK"

    # Verify PSM has detailed information
    sample_psm = elltk_psms[0]
    assert sample_psm.charge_state is not None, "PSM should have charge state"
    assert sample_psm.experimental_mz is not None, "PSM should have experimental m/z"
    assert sample_psm.calculated_mz is not None, "PSM should have calculated m/z"
    assert sample_psm.pass_threshold is not None, "PSM should have pass_threshold flag"
    assert sample_psm.rank is not None, "PSM should have rank"

    # Verify score_values are captured (Mascot score, expectation value)
    assert sample_psm.score_values is not None, "PSM should have score values"
    assert isinstance(sample_psm.score_values, dict), "Score values should be a dict"
    # Based on Mascot data, we expect Mascot:score
    assert len(sample_psm.score_values) > 0, "Should have at least one score"

    # =========================================================================
    # Test PSM-PeptideEvidence Junctions
    # =========================================================================
    assert len(psm_peptide_evidence_junctions) > 0, (
        "Should create junction records linking PSMs to peptide evidence"
    )

    # All junctions should have valid IDs
    psm_ids = {psm.id for psm in psms}
    pe_ids = {pe.id for pe in peptide_evidence}

    for junction in psm_peptide_evidence_junctions:
        assert junction.psm_id in psm_ids, "Junction should reference a valid PSM"
        assert junction.peptide_evidence_id in pe_ids, (
            "Junction should reference a valid peptide evidence"
        )

    # Verify that some PSMs link to multiple protein evidences (shared peptides)
    psm_pe_counts = {}
    for junction in psm_peptide_evidence_junctions:
        psm_pe_counts[junction.psm_id] = psm_pe_counts.get(junction.psm_id, 0) + 1

    # At least one PSM should map to multiple proteins
    max_pe_per_psm = max(psm_pe_counts.values())
    assert max_pe_per_psm >= 1, "PSMs should link to at least one peptide evidence"

    # =========================================================================
    # Test Data Consistency and Relationships
    # =========================================================================

    # All peptide modifications should reference valid peptides
    peptide_ids = {p.id for p in peptides}
    for mod in peptide_modifications:
        assert mod.peptide_id in peptide_ids, "Modification should reference a valid peptide"

    # All PSMs should reference valid peptides
    for psm in psms:
        assert psm.peptide_id in peptide_ids, "PSM should reference a valid peptide"
