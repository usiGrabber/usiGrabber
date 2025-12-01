from usigrabber.file_parser.txt_zip.parsing_functions import (
    link_modifications,
    parse_peptide_evidence,
    parse_peptides,
    parse_psms,
)


def test_parse_peptides_basic(project2_evidence_df, project2_peptides_df):
    """
    Basic test for parse_peptides function to ensure it correctly parses
    peptide sequences and modifications from provided DataFrames.
    """
    peptide_id_map, peptide_mods, peptides = parse_peptides(
        project2_evidence_df, project2_peptides_df
    )

    assert len(peptides) == 4
    assert len(peptide_id_map) == 4
    assert len(peptide_mods) == 4
    mod_count = 0
    for mods in peptide_mods.values():
        mod_count += len(mods)
    assert mod_count == 6
    sequences = {p.sequence for p in peptides}
    assert "AAAAAAAAAAAAGDSDSWDADTFSMEDPVR" in sequences
    assert "AAAAAAAAAAGDSDSWDADTFSMEDPVR" in sequences


def test_parse_peptides(project1_evidence_df, project1_peptides_df):
    """
    Comprehensive test for parse_peptides function to validate parsing logic
    and data integrity.
    """
    peptide_id_map, peptide_mods, peptides = parse_peptides(
        project1_evidence_df, project1_peptides_df
    )

    assert len(peptides) == 1174
    assert len(peptide_id_map) == 1174
    assert len(peptide_mods) == 140
    mod_count = 0
    for mods in peptide_mods.values():
        mod_count += len(mods)
    assert mod_count == 148
    sequences = {p.sequence for p in peptides}
    assert "AAAALKGSDHR" in sequences
    assert "ALEYKDFDKFDR" in sequences
    assert "CKHFEIGGDKK" in sequences


def test_parse_peptide_evidence_basic(project2_peptides_df):
    """
    Basic test for parse_peptide_evidence function to ensure it correctly
    parses peptide evidence from provided DataFrame.
    """
    pe_id_map, peptide_evidence = parse_peptide_evidence(project2_peptides_df)

    assert len(peptide_evidence) > 0
    assert len(pe_id_map) > 0
    assert len([pe_id for pe_id in pe_id_map if len(pe_id) > 0]) <= len(peptide_evidence)
    assert "Q66JS6" in {pe.protein_accession for pe in peptide_evidence}
    assert "Q3UGC7" in {pe.protein_accession for pe in peptide_evidence}

    pe_with_positions = [pe for pe in peptide_evidence if pe.start_position is not None]
    assert len(pe_with_positions) > 0, "Some peptide evidence should have position info"

    for pe in pe_with_positions:
        if pe.start_position is not None:
            assert pe.start_position > 0, "Start position should be positive"
            if pe.end_position is not None:
                assert pe.end_position >= pe.start_position, (
                    "End position should be >= start position"
                )


def test_parse_peptide_evidence(project1_peptides_df):
    """
    Comprehensive test for parse_peptide_evidence function to validate
    parsing logic and data integrity.
    """
    pe_id_map, peptide_evidence = parse_peptide_evidence(project1_peptides_df)

    assert len(peptide_evidence) > 0
    assert len(pe_id_map) > 0
    assert len([pe_id for pe_id in pe_id_map if len(pe_id) > 0]) <= len(peptide_evidence)
    assert "sp|O50008|METE1_ARATH" in {pe.protein_accession for pe in peptide_evidence}
    assert "sp|Q08770|RL102_ARATH" in {pe.protein_accession for pe in peptide_evidence}

    pe_with_positions = [pe for pe in peptide_evidence if pe.start_position is not None]
    assert len(pe_with_positions) > 0, "Some peptide evidence should have position info"

    for pe in pe_with_positions:
        if pe.start_position is not None:
            assert pe.start_position > 0, "Start position should be positive"
            if pe.end_position is not None:
                assert pe.end_position >= pe.start_position, (
                    "End position should be >= start position"
                )


def test_parse_psms_basic(
    project2_evidence_df,
    project2_summary_df,
    project2_peptides_df,
):
    """
    Basic test for parse_psms function to ensure it correctly parses
    PSMs and related data from provided DataFrames.
    """
    peptide_id_map, _, _ = parse_peptides(project2_evidence_df, project2_peptides_df)
    pe_id_map, _ = parse_peptide_evidence(project2_peptides_df)

    psm_batch, junction_batch, search_mod_batch = parse_psms(
        project2_evidence_df,
        project2_summary_df,
        "PXD000001",
        peptide_id_map,
        pe_id_map,
    )

    assert len(psm_batch) > 0
    assert len(psm_batch) <= 25
    assert len(junction_batch) > len(pe_id_map)
    for psm in psm_batch:
        assert (
            len([search_mod for search_mod in search_mod_batch if search_mod.psm_id == psm.id]) == 3
        )


def test_parse_psms(
    project1_evidence_df,
    project1_summary_df,
    project1_peptides_df,
):
    """
    Comprehensive test for parse_psms function to validate parsing logic
    and data integrity.
    """
    peptide_id_map, _, _ = parse_peptides(project1_evidence_df, project1_peptides_df)
    pe_id_map, _ = parse_peptide_evidence(project1_peptides_df)

    psm_batch, junction_batch, search_mod_batch = parse_psms(
        project1_evidence_df,
        project1_summary_df,
        "PXD000002",
        peptide_id_map,
        pe_id_map,
    )

    assert len(psm_batch) > 0
    assert len(psm_batch) <= 2000
    assert len(junction_batch) > len(pe_id_map)
    for psm in psm_batch:
        assert (
            len([search_mod for search_mod in search_mod_batch if search_mod.psm_id == psm.id]) == 4
        )


def test_link_modifications_basic(project2_evidence_df, project2_peptides_df):
    """
    Basic test for link_modifications function to ensure it correctly
    links peptide modifications to their respective peptides.
    """
    _, peptide_mods, _ = parse_peptides(
        project2_evidence_df,
        project2_peptides_df,
    )

    mod_batch = link_modifications(peptide_mods)

    assert len(mod_batch) > 0
    for mod in mod_batch:
        assert mod.unimod_id is not None
        assert mod.position is not None
