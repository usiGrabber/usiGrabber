"""
mzID Helper Functions

Pure parsing functions with no database dependencies.
These functions extract and transform data from mzIdentML elements.
"""

import re
import subprocess
from collections import defaultdict
from pathlib import Path
from string import digits
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

from usigrabber.db.schema import IndexType
from usigrabber.utils import logger, lookup_unimod_id_by_name


def extract_unimod_id_or_name(mod_data: dict) -> tuple[int | None, str | None]:
    """
    Extract UNIMOD ID and name from modification data.

    Args:
            mod_data: Modification dictionary

    Returns:
            UNIMOD ID as integer, or None if not found.
            Returns modification name if unimod ID not found and name is available.

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
                    return int(accession.split(":")[-1]), None
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
        return None, mod_name
    else:
        return uid, None


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
    Extract modification position and residue information.

    Args:
            mod: Modification dictionary

    Returns:
            Tuple of (position, residues_string)
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


def extract_index_type_and_number(sir: dict) -> tuple[IndexType | None, int | None]:
    """
    Extract USI-related fields from SpectrumIdentificationResult.

    Parses spectrum title (MS:1000796) or scan number (MS:1001115) cvParams to extract:
    - index_type: Type of spectrum index (scan, index, nativeId, or trace)
    - index_number: Spectrum index number

    Args:
        sir: SpectrumIdentificationResult dictionary

    Returns:
        Tuple of (index_type, index_number)
    """
    index_type: IndexType | None = None
    index_number: int | None = None

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
    if spectrum_title and "scan=" in spectrum_title:
        # Example format:
        # "OTE0019_York_060813_JH16.3285.3285.2 File:\"OTE0019_York_060813_JH16.raw\",
        #  NativeID:\"controllerType=0 controllerNumber=1 scan=3285\""

        # Extract scan number from NativeID if present (overwrites existing spectrum id)
        try:
            match = re.search(r"scan=(\d+)", spectrum_title)
            if match:
                index_number = int(match.group(1))
            index_type = IndexType.scan
        except (ValueError, IndexError):
            pass

    # Parse scan number(s) (MS:1001115)
    if scan_number_value and index_type is None:
        try:
            index_number = int(scan_number_value)
            index_type = IndexType.scan
        except (ValueError, TypeError):
            pass

    return index_type, index_number


SPECTRUM_ID_FORMAT_MAPPING = {
    "MS:1000774": IndexType.index,
    "MS:1000776": IndexType.scan,
    "MS:1000768": IndexType.scan,
    "MS:1001530": IndexType.nativeId,
}

# Set to track already logged exceptions
# WARNING: not thread-safe, but acceptable for this use case
exceptions = set()


def get_spectrum_id_format(cv_param: str) -> IndexType | None:
    """
    Map SpectrumIDFormat accession to human-readable format.

    Args:
        cv_param: SpectrumIDFormat cvParam accession

    Returns:
        Human-readable format string or None if not found
    """
    id_format = SPECTRUM_ID_FORMAT_MAPPING.get(cv_param)
    if not id_format:
        if cv_param not in exceptions:
            # only print each exception once
            logger.warning("Unknown SpectrumIDFormat accession: %s", cv_param)
            exceptions.add(cv_param)
        return None
    return id_format


def extract_xml_subtree(xml_path: Path, tag: str) -> str:
    """
    Use sed to extract XML subtree from a file and return as string.
    """
    cmd = ["sed", "-n", rf"/<{tag}>/,/<\/{tag}>/p", str(xml_path)]
    proc = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        check=True,
    )
    return proc.stdout.strip()


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
        {'MOD1': [3, 9], 'MOD2': [6]}
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


def simple_mod_name(mod_name: str) -> str:
    """
    Simplify a modification name by removing leading numbers and spaces.
    This function takes a modification name string and removes any leading
    numeric characters and spaces to return a cleaner version of the name.
    Args:
        mod_name (str): The original modification name (i.e. Oxidation (M)).
    Returns:
        str: The simplified modification name (i.e. Oxidation).
    """
    mod_name = str(mod_name)
    return mod_name.lstrip(digits).lstrip(" ").split(" ")[0]


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
            logger.debug(
                f"Found a unique triplet of (evidence.txt, summary.txt, peptides.txt) in {parent}"
            )
        else:
            logger.debug(
                "Could not find a unique triplet of (evidence.txt, summary.txt, peptides.txt) in "
                f"{parent}"
            )

    return triplets


def get_db_insert_function(engine: Engine):
    # Detect database type to use appropriate insert dialect
    db_dialect = engine.dialect.name
    is_postgresql = db_dialect == "postgresql"

    # Select appropriate insert function based on database type
    return pg_insert if is_postgresql else sqlite_insert
