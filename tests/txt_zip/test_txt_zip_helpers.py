from pathlib import Path

from usigrabber.file_parser.txt_zip.helpers import (
    clean_mod_list_of_numbers,
    clear_mod_name,
    extract_mods,
    get_mods_with_positions,
    get_residues_for_mods_with_positions,
    get_txt_triples,
    remove_brackets_before_index,
)


def test_remove_brackets_before_index():
    modified_sequence = "(Acetyl (Protein N-term))ANAASGM(Oxidation (M))AVHDDCKLK"
    cleaned_sequence = remove_brackets_before_index(
        modified_sequence, modified_sequence.find("Oxidation (M)")
    )
    assert cleaned_sequence == modified_sequence.replace("(Acetyl (Protein N-term))", "")

    modified_sequence = "(Acetyl (Protein N-term))ANAASGMAVHDDCKLK"
    cleaned_sequence = remove_brackets_before_index(
        modified_sequence, modified_sequence.find("Acetyl (Protein N-term)")
    )
    assert cleaned_sequence == modified_sequence

    modified_sequence = "ANAASGM(Oxidation (M))AVHDDC(Oxidation (M))KLK"
    cleaned_sequence = remove_brackets_before_index(
        modified_sequence, modified_sequence.find("Oxidation (M)")
    )
    assert cleaned_sequence == modified_sequence


def test_get_mods_with_positions():
    seq = "(Acetyl (Protein N-term))ANAASGM(Oxidation (M))AVHDDCKLK"
    mods = ["Acetyl (Protein N-term)", "Oxidation (M)"]
    mod_with_pos, seq = get_mods_with_positions(seq, mods)
    assert len(mod_with_pos.keys()) == 2
    assert mod_with_pos["Acetyl (Protein N-term)"][0] == 0
    assert mod_with_pos["Oxidation (M)"][0] == 7
    assert seq == "ANAASGMAVHDDCKLK"

    seq = "(Acetyl (Protein N-term))ANAASGM(Oxidation (M))AVHDDC(Oxidation (M))KLK"
    mods = ["Acetyl (Protein N-term)", "Oxidation (M)"]
    mod_with_pos, seq = get_mods_with_positions(seq, mods)
    assert len(mod_with_pos.keys()) == 2
    assert mod_with_pos["Acetyl (Protein N-term)"][0] == 0
    assert len(mod_with_pos["Oxidation (M)"]) == 2
    assert mod_with_pos["Oxidation (M)"][0] == 7
    assert mod_with_pos["Oxidation (M)"][1] == 13
    assert seq == "ANAASGMAVHDDCKLK"


def test_get_residues_for_mods_with_positions():
    seq = (
        "(Acetyl (Protein N-term))ANAASGM(Oxidation (M))"
        + "AVHDDC(Oxidation (M))KLK(Phospho (Protein T-term))"
    )
    mods = ["Acetyl (Protein N-term)", "Oxidation (M)", "Phospho (Protein T-term)"]
    mod_with_pos, seq = get_mods_with_positions(seq, mods)
    mod_with_pos_residues = get_residues_for_mods_with_positions(seq, mods, mod_with_pos)
    assert len(mod_with_pos_residues.keys()) == 3
    assert mod_with_pos_residues["Acetyl (Protein N-term)"][0][0] == 0
    assert mod_with_pos_residues["Acetyl (Protein N-term)"][0][1] == "N"
    assert mod_with_pos_residues["Oxidation (M)"][0][0] == 7
    assert mod_with_pos_residues["Oxidation (M)"][0][1] == "M"
    assert mod_with_pos_residues["Phospho (Protein T-term)"][0][0] == 16
    assert mod_with_pos_residues["Phospho (Protein T-term)"][0][1] == "T"


def test_clear_mod_name():
    mods_with_pos_residues = {
        "Acetyl (Protein N-term)": [(0, "N")],
        "Oxidation (M)": [(7, "M"), (13, "M")],
        "Phospho (Protein T-term)": [(16, "T")],
    }
    modnames_with_pos_residues = clear_mod_name(mods_with_pos_residues)
    assert len(modnames_with_pos_residues.keys()) == 3
    assert modnames_with_pos_residues["Acetyl"][0][0] == 0
    assert modnames_with_pos_residues["Acetyl"][0][1] == "N"
    assert modnames_with_pos_residues["Oxidation"][0][0] == 7
    assert modnames_with_pos_residues["Oxidation"][0][1] == "M"
    assert modnames_with_pos_residues["Phospho"][0][0] == 16
    assert modnames_with_pos_residues["Phospho"][0][1] == "T"


def test_extract_mods():
    seq = "(Acetyl (Protein N-term))ANAASGM(Oxidation (M))AVHDDC(Oxidation (M))KLK"
    mods = ["Acetyl (Protein N-term)", "Oxidation (M)"]
    mod_dict = extract_mods(seq, mods)
    assert len(mod_dict.keys()) == 2
    assert mod_dict["Acetyl"][0][0] == 0
    assert mod_dict["Acetyl"][0][1] == "N"
    assert mod_dict["Oxidation"][0][0] == 7
    assert mod_dict["Oxidation"][0][1] == "M"


def test_clean_mod_list_of_numbers():
    mod_list = ["Acetyl (Protein N-term)", "Oxidation (M)"]
    cleaned_mod_list = clean_mod_list_of_numbers(mod_list)
    assert cleaned_mod_list[0] == mod_list[0]
    assert cleaned_mod_list[1] == mod_list[1]

    mod_list = ["Acetyl (Protein N-term)", "2 Oxidation (M)"]
    cleaned_mod_list = clean_mod_list_of_numbers(mod_list)
    assert cleaned_mod_list[0] == mod_list[0]
    assert cleaned_mod_list[1] == "Oxidation (M)"


def test_get_txt_triples():
    files_ordered = [
        Path("tests/txt_zip/fixtures/project1/evidence.txt"),
        Path("tests/txt_zip/fixtures/project1/summary.txt"),
        Path("tests/txt_zip/fixtures/project1/peptides.txt"),
        Path("tests/txt_zip/fixtures/project1/decoy_file.txt"),
        Path("tests/txt_zip/fixtures/project2/evidence.txt"),
        Path("tests/txt_zip/fixtures/project2/summary.txt"),
        Path("tests/txt_zip/fixtures/project2/peptides.txt"),
        Path("tests/txt_zip/fixtures/project2/decoy_file.txt"),
        Path("tests/txt_zip/fixtures/decoy_file.txt"),
    ]

    files_shuffled = [
        Path("tests/txt_zip/fixtures/project2/peptides.txt"),
        Path("tests/txt_zip/fixtures/project1/summary.txt"),
        Path("tests/txt_zip/fixtures/decoy_file.txt"),
        Path("tests/txt_zip/fixtures/project1/peptides.txt"),
        Path("tests/txt_zip/fixtures/project2/decoy_file.txt"),
        Path("tests/txt_zip/fixtures/project1/decoy_file.txt"),
        Path("tests/txt_zip/fixtures/project2/summary.txt"),
        Path("tests/txt_zip/fixtures/project1/evidence.txt"),
        Path("tests/txt_zip/fixtures/project2/evidence.txt"),
    ]

    txt_triple_ordered = get_txt_triples(files_ordered)
    txt_triple_shuffled = get_txt_triples(files_shuffled)

    assert len(txt_triple_ordered) == len(txt_triple_shuffled)
    assert len(txt_triple_ordered) == 2

    for triple in txt_triple_ordered:
        evidence, summary, peptides = triple
        assert evidence.parent == summary.parent == peptides.parent

    for triple in txt_triple_shuffled:
        assert triple in txt_triple_ordered
