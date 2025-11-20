from string import digits
from typing import Any, cast

from pyteomics.mass.unimod import Modification

from usigrabber.utils import get_unimod_db


def remove_brackets_before_index(s: str, cut_index: int) -> str:
    result = []
    i = 0
    n = len(s)

    depth = 0
    while i < n:
        if s[i] == "(" and i < cut_index:
            depth += 1
        elif s[i] == ")" and i < cut_index:
            depth -= 1
        elif depth == 0:
            result.append(s[i])
        i += 1
    return "".join(result)


def extract_mods(sequence: str, mods: list[str]) -> dict[str, list[tuple[int, str]]]:
    mod_with_pos: dict[str, list[int]] = {mod: [] for mod in mods}
    seq = sequence.strip("_")

    for mod in mods:
        while seq.find(f"({mod})") != -1:
            cleaned_seq = remove_brackets_before_index(seq, seq.find(f"({mod})"))
            mod_index = cleaned_seq.find(f"({mod})")
            mod_with_pos[mod].append(mod_index)

            seq_index = seq.find(f"({mod})")
            seq = seq[:seq_index] + seq[seq_index + len(f"({mod})") :]

    mod_with_pos_residues: dict[str, list[tuple[int, str]]] = {mod: [] for mod in mods}
    for mod, positions in mod_with_pos.items():
        for position in positions:
            residue = ""
            if position == 0:
                residue = seq[0]
            elif position == len(seq):
                residue = seq[-1]
            else:
                residue = seq[position - 1 : position + 1]
            mod_with_pos_residues[mod].append((position, residue))
    return mod_with_pos_residues


def clean_mod_list_of_numbers(mod_list: list[str]) -> list[str]:
    cleaned_mods = []
    for mod in mod_list:
        cleaned_mods.append(mod.lstrip(digits).lstrip(" "))
    return cleaned_mods


def extract_unimod_id(mod: str) -> int | None:
    mod.split(" ")[0]
    try:
        unimod: Any | Modification | None = get_unimod_db().get(mod)
        if unimod is not None:
            try:
                return int(cast(int, unimod.id))
            except (TypeError, ValueError):
                # If mod.id cannot be converted to int (e.g., a SQLAlchemy Column), skip
                pass
    except KeyError:
        pass


def get_unimod_id(mod: str) -> int | None:
    return extract_unimod_id(mod)
