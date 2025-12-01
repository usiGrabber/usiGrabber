"""
mzID Helper Functions

Pure parsing functions with no database dependencies.
These functions extract and transform data from mzIdentML elements.
"""

from collections import defaultdict
from pathlib import Path
from string import digits
from typing import Any

from usigrabber.db.schema import IndexType
from usigrabber.utils import logger, lookup_unimod_id_by_name


def extract_unimod_id_and_name(mod_data: dict) -> tuple[int | None, str | None]:
    """
    Extract UNIMOD ID and name from modification data.

    Args:
            mod_data: Modification dictionary

    Returns:
            UNIMOD ID as integer, or None if not found. Also returns modification name.

    """
    # Check if cvParam exists
    cv_params = mod_data.get("cvParam")

    mod_name = mod_data.get("name")
    uid: int | None = None

    if cv_params:
        # cvParam can be a list or a single dict
        if not isinstance(cv_params, list):
            cv_params = [cv_params]

        # Look for UNIMOD accession
        for param in cv_params:
            accession = param.get("accession", "")
            if "UNIMOD:" in accession and len(accession) > 7:
                try:
                    # Extract number from "UNIMOD:35" format
                    return int(accession.split(":")[-1]), mod_name
                except (ValueError, IndexError):
                    pass
            name = param.get("name", "")
            if name:
                # Fallback: resolve by modification name
                uid = lookup_unimod_id_by_name(name)

    if mod_name:
        # Fallback: resolve by modification name
        uid = lookup_unimod_id_by_name(mod_name)

    if uid is None:
        logger.debug("No UNIMOD ID found for modification: %s", mod_data)
    return uid, mod_name


def extract_score_values(sii: dict) -> dict[str, Any]:
    """
    Extract score values from a SpectrumIdentificationItem.

    Args:
            sii: SpectrumIdentificationItem dictionary

    Returns:
            Dictionary of score names to values
    """
    score_values: dict[str, Any] = {}

    for key, value in sii.items():
        # Include fields that look like scores, expectation values, etc.
        key_lower = key.lower()
        if any(term in key_lower for term in ["score", "value", "fdr", "qvalue", "expect"]):
            # Skip fields that are not actual score values
            if key in ["peptide_ref", "PeptideEvidenceRef", "passThreshold"]:
                continue
            score_values[key] = value

    return score_values if score_values else {}


def parse_modification_location(mod: dict) -> tuple[int | None, str | None]:
    """
    Extract modification location and residue information.

    Args:
            mod: Modification dictionary

    Returns:
            Tuple of (location, residues_string)
    """
    location = mod.get("location")
    residues = mod.get("residues")

    # Convert residues to string if it's a list
    if isinstance(residues, list):
        residues = "".join(residues)

    return location, residues


def normalize_residues(residues: Any) -> str:
    """
    Normalize residue representation to string.

    Args:
            residues: Residues in various formats (string, list, etc.)

    Returns:
            Normalized residue string
    """
    if isinstance(residues, list):
        return "".join(residues)
    return str(residues) if residues else ""


def extract_usi_fields(sir: dict) -> tuple[IndexType | None, int | None, str | None]:
    """
    Extract USI-related fields from SpectrumIdentificationResult.

    Parses spectrum title (MS:1000796) or scan number (MS:1001115) cvParams to extract:
    - index_type: Type of spectrum index (scan, index, nativeId, or trace)
    - index_number: Spectrum index number
    - ms_run: MS run identifier from raw file name

    Args:
            sir: SpectrumIdentificationResult dictionary

    Returns:
            Tuple of (index_type, index_number, ms_run)
    """
    index_type: IndexType | None = None
    index_number: int | None = None
    ms_run: str | None = None

    # First try to parse from spectrumID attribute
    spectrum_id = sir.get("spectrumID", "")
    if spectrum_id:
        # Check for "index=" pattern
        if spectrum_id.startswith("index="):
            try:
                index_type = IndexType.index
                index_number = int(spectrum_id.split("=")[1])
            except (ValueError, IndexError):
                pass
        # Check for "scan=" pattern
        elif spectrum_id.startswith("scan="):
            try:
                index_type = IndexType.scan
                index_number = int(spectrum_id.split("=")[1])
            except (ValueError, IndexError):
                pass
        else:
            # Fallback: try to parse integer directly
            try:
                index_number = int(spectrum_id)
                index_type = IndexType.scan
            except ValueError:
                pass

    spectrum_title: str | None = sir.get("spectrum title")
    scan_number_value: str | None = sir.get("scan number(s)")

    # Parse spectrum title (MS:1000796)
    if spectrum_title:
        # Example format:
        # "OTE0019_York_060813_JH16.3285.3285.2 File:\"OTE0019_York_060813_JH16.raw\",
        #  NativeID:\"controllerType=0 controllerNumber=1 scan=3285\""

        # Extract scan number from NativeID if present (overwrites existing spectrum id)
        if "scan=" in spectrum_title:
            try:
                scan_part = spectrum_title.split("scan=")[1]
                scan_num_str = scan_part.split('"')[0].split()[0]
                index_number = int(scan_num_str)
                index_type = IndexType.scan
            except (ValueError, IndexError):
                pass

        # Extract MS run from File field
        if 'File:"' in spectrum_title or "File:&quot;" in spectrum_title:
            try:
                # Handle both " and &quot; encodings
                file_part = spectrum_title.split("File:")[1]
                if file_part.startswith("&quot;"):
                    file_name = file_part.split("&quot;")[1]
                else:
                    file_name = file_part.split('"')[1]
                # Remove .raw extension
                ms_run = file_name.replace(".raw", "").replace(".RAW", "")
            except IndexError:
                pass

    # Parse scan number(s) (MS:1001115)
    if scan_number_value and index_type is None:
        try:
            index_type = IndexType.scan
            index_number = int(scan_number_value)
        except (ValueError, TypeError):
            pass

    return index_type, index_number, ms_run


def log_info(logger, stats, file_name: str) -> None:
    """
    Log import summary information.
    Args:
            logger: Logger instance
            stats: ImportStats object
            file_name: Name of the imported file
    """
    duration_str = f"{stats.duration_seconds:.1f}s" if stats.duration_seconds is not None else "N/A"
    logger.info(f"Imported {stats.psm_count:,} PSMs from '{file_name}' ({duration_str})")


def remove_brackets_before_index(s: str, cut_index: int) -> str:
    """
    Remove parentheses and their contents before a specified index in a string.
    This function removes all opening and closing brackets that appear before
    the specified cut_index position, along with any text contained within those
    brackets. Text at or after the cut_index is preserved, even if it contains
    brackets.
    Args:
        s (str): The input string to process.
        cut_index (int): The index position before which brackets should be removed.
                        Positions at or after this index are preserved.
    Returns:
        str: A new string with brackets and their contents removed from positions
             before cut_index, while preserving all text from cut_index onwards.
    Example:
        >>> remove_brackets_before_index("hello(world)test", 5)
        "hello(world)test"
        >>> remove_brackets_before_index("(a)b(c)d", 5)
        "b(c)d"
    """

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


def get_mods_with_positions(seq: str, mods: list[str]) -> tuple[dict[str, list[int]], str]:
    """
    Extract modifications and their positions from a sequence string.
    This function searches for modifications (specified in the mods list) within a sequence string.
    Each modification is expected to be enclosed in parentheses, e.g., "(MOD_NAME)".
    The function identifies all occurrences of each modification, records their positions,
    and removes them from the sequence.
    Args:
        seq (str): The sequence string containing modifications in the format "(MOD_NAME)".
        mods (list[str]): A list of modification identifiers to search for in the sequence.
    Returns:
        tuple[dict[str, list[int]], str]: A tuple containing:
            - A dictionary mapping each modification name to a list of positions where it was found.
            - The cleaned sequence string with all modifications removed.
    Example:
        >>> seq = "ABC(MOD1)DEF(MOD2)GHI(MOD1)"
        >>> mods = ["MOD1", "MOD2"]
        >>> positions, clean_seq = get_mods_with_positions(seq, mods)
        >>> positions
        {'MOD1': [3, 10], 'MOD2': [7]}
        >>> clean_seq
        'ABCDEFGHI'
    """

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
    seq: str, mod_with_pos: dict[str, list[int]]
) -> dict[str, list[tuple[int, str]]]:
    """
    Map modification types to their positions and corresponding residues in a sequence.
    This function takes a sequence and modification data, then retrieves the amino acid
    residue at each modification position. Special handling is applied for terminal
    positions: position 0 represents the N-terminus (marked as 'N') and position equal
    to sequence length represents the C-terminus (marked as 'C').
    Args:
        seq (str): The amino acid sequence.
        mods (list[str]): List of modification types to initialize in the result dictionary.
        mod_with_pos (dict[str, list[int]]): Dictionary mapping modification types to lists
                                             of positions where they occur in the sequence.
    Returns:
        dict[str, list[tuple[int, str]]]: Dictionary mapping each modification type to a list
                                          of tuples containing (position, residue) pairs.
                                          Terminal residues are represented as 'N' for N-terminus
                                          and 'C' for C-terminus.
    Example:
        >>> seq = "PEPTIDE"
        >>> mod_with_pos = {"Phospho": [0, 4], "Acetyl": [7]}
        >>> get_residues_for_mods_with_positions(seq, mods, mod_with_pos)
        {'Phospho': [(0, 'N'), (4, 'T')], 'Acetyl': [(7, 'C')]}
    """
    mod_with_pos_residues: dict[str, list[tuple[int, str]]] = {mod: [] for mod in mod_with_pos}

    for mod, positions in mod_with_pos.items():
        for position in positions:
            residue = ""
            if position == 0:
                residue = "N"
            elif position == len(seq):
                residue = "C"
            else:
                residue = seq[position - 1 : position]
            mod_with_pos_residues[mod].append((position, residue))

    return mod_with_pos_residues


def clear_mod_name(
    mods_with_pos_residues: dict[str, list[tuple[int, str]]],
) -> dict[str, list[tuple[int, str]]]:
    """
    Simplify modification names by removing additional details.
    This function processes a dictionary mapping modification names to their positions
    and corresponding residues. It simplifies each modification name by retaining only
    the primary identifier (the substring before the first space) and constructs a new
    dictionary with these simplified names.
    Args:
        mods_with_pos_residues (dict[str, list[tuple[int, str]]]): A dictionary mapping
            full modification names to lists of (position, residue) tuples.
    Returns:
        dict[str, list[tuple[int, str]]]: A new dictionary mapping simplified modification
            names to lists of (position, residue) tuples.
    """
    modnames_with_pos_residues: dict[str, list[tuple[int, str]]] = {}
    for mod, position_residue in mods_with_pos_residues.items():
        clean_mod_name = mod.split(" ")[0]
        modnames_with_pos_residues[clean_mod_name] = position_residue
    return modnames_with_pos_residues


def extract_mods(sequence: str, mods: list[str]) -> dict[str, list[tuple[int, str]]]:
    """
    Extract and simplify modification data from a sequence string.
    This function identifies modifications within a sequence string, retrieves their
    positions and corresponding residues, and simplifies the modification names.
    Args:
        sequence (str): The sequence string containing modifications.
        mods (list[str]): A list of modification identifiers to search for in the sequence.
    Returns:
        dict[str, list[tuple[int, str]]]: A dictionary mapping simplified modification
            names to lists of (position, residue) tuples.
    """
    seq = str(sequence).strip("_")

    mod_with_pos, seq = get_mods_with_positions(seq, mods)
    mod_with_pos_residues = get_residues_for_mods_with_positions(seq, mod_with_pos)
    modnames_with_pos_residues = clear_mod_name(mod_with_pos_residues)

    return modnames_with_pos_residues


def clean_mod_list_of_numbers(mod_list: list[str]) -> list[str]:
    """
    Remove leading numbers and spaces from modification names in a list.
    This function processes a list of modification names, stripping any leading
    numeric characters and spaces from each name.
    Args:
        mod_list (list[str]): A list of modification names, potentially with leading numbers.
    Returns:
        list[str]: A new list of modification names with leading numbers and spaces removed.
    """
    cleaned_mods = []
    for mod in mod_list:
        mod = str(mod)
        cleaned_mods.append(mod.lstrip(digits).lstrip(" "))
    return cleaned_mods


def get_txt_triples(files: list[Path]):
    """
    Given a list of file paths, group them into triplets of
    (evidence.txt, summary.txt, peptides.txt) based on their parent directory.
    Args:
        files (list[Path]): A list of file paths to process.
    Returns:
        list[tuple[Path, Path, Path]]: A list of triplets, each containing the paths
            to evidence.txt, summary.txt, and peptides.txt files from the same directory.
    """
    # group files by parent directory
    grouped = defaultdict(list)
    for f in files:
        grouped[f.parent].append(f)

    triplets = []

    for parent, flist in grouped.items():
        evidence_files = [f for f in flist if f.name.endswith("evidence.txt")]
        summary_files = [f for f in flist if f.name.endswith("summary.txt")]
        peptides_files = [f for f in flist if f.name.endswith("peptides.txt")]

        if len(evidence_files) == 1 and len(summary_files) == 1 and len(peptides_files) == 1:
            triplets.append((evidence_files[0], summary_files[0], peptides_files[0]))
        else:
            logger.warning(
                "Could not find a unique triplet of (evidence.txt, summary.txt, peptides.txt)"
                f" in {parent}"
            )

    return triplets
