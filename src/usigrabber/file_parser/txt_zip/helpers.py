from collections import defaultdict
from pathlib import Path
from string import digits


def remove_brackets_before_index(s: str, cut_index: int) -> str:
    result = []
    i = 0
    n = len(s)
    cut_index -= 1

    depth = 0
    while i < n:
        if s[i] == "(" and i < cut_index:
            depth += 1
        elif s[i] == ")" and i < cut_index:
            depth -= 1
        elif depth == 0 or i >= cut_index:
            result.append(s[i])
        i += 1
    return "".join(result)


def get_mods_with_positions(seq: str, mods: list[str]):
    mod_with_pos: dict[str, list[int]] = {mod: [] for mod in mods}
    for mod in mods:
        while seq.find(f"({mod})") != -1:
            cleaned_seq = remove_brackets_before_index(seq, seq.find(f"({mod})"))
            mod_index = cleaned_seq.find(f"({mod})")
            mod_with_pos[mod].append(mod_index)

            seq_index = seq.find(f"({mod})")
            seq = seq[:seq_index] + seq[seq_index + len(f"({mod})") :]
    return mod_with_pos, seq


def get_residues_for_mods_with_positions(
    seq: str, mods: list[str], mod_with_pos: dict[str, list[int]]
):
    mod_with_pos_residues: dict[str, list[tuple[int, str]]] = {mod: [] for mod in mods}

    for mod, positions in mod_with_pos.items():
        for position in positions:
            residue = ""
            if position == 0:
                residue = "N"
            elif position == len(seq):
                residue = "T"
            else:
                residue = seq[position - 1 : position]
            mod_with_pos_residues[mod].append((position, residue))

    return mod_with_pos_residues


def clear_mod_name(
    mods_with_pos_residues: dict[str, list[tuple[int, str]]],
) -> dict[str, list[tuple[int, str]]]:
    modnames_with_pos_residues: dict[str, list[tuple[int, str]]] = {}
    for mod, position_residue in mods_with_pos_residues.items():
        clean_mod_name = mod.split(" ")[0]
        modnames_with_pos_residues[clean_mod_name] = position_residue
    return modnames_with_pos_residues


def extract_mods(sequence: str, mods: list[str]) -> dict[str, list[tuple[int, str]]]:
    seq = str(sequence).strip("_")

    mod_with_pos, seq = get_mods_with_positions(seq, mods)
    mod_with_pos_residues = get_residues_for_mods_with_positions(seq, mods, mod_with_pos)
    modnames_with_pos_residues = clear_mod_name(mod_with_pos_residues)

    return modnames_with_pos_residues


def clean_mod_list_of_numbers(mod_list: list[str]) -> list[str]:
    cleaned_mods = []
    for mod in mod_list:
        mod = str(mod)
        cleaned_mods.append(mod.lstrip(digits).lstrip(" "))
    return cleaned_mods


def get_txt_triples(files: list[Path]):
    # group files by parent directory
    grouped = defaultdict(list)
    for f in files:
        grouped[f.parent].append(f)

    triplets = []

    for _, flist in grouped.items():
        evidence_files = [f for f in flist if f.name.endswith("evidence.txt")]
        summary_files = [f for f in flist if f.name.endswith("summary.txt")]
        peptides_files = [f for f in flist if f.name.endswith("peptides.txt")]

        if len(evidence_files) == 1 and len(summary_files) == 1 and len(peptides_files) == 1:
            triplets.append((evidence_files[0], summary_files[0], peptides_files[0]))

    return triplets
