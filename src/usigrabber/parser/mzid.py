import re
from collections import defaultdict
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any, TypedDict

from pyteomics import mzid

from usigrabber.parser.base import USIGenerator
from usigrabber.utils import data_directory_path, get_unimod_db, logger


class Mod(TypedDict):
    location: int
    residue: str | None
    name: str


Mods = list[Mod]


class MZID(USIGenerator):
    @classmethod
    def _replace_with_unimod(cls, mod_name: str) -> str:
        """
        Replace a modification name with its Unimod identifier if available.

        :param mod_name: Modification name to look up.
        :type mod_name: str
        :returns: Unimod identifier in the format "UNIMOD:<id>" or the original name if not found.
        :rtype: str
        """
        unimod_db = get_unimod_db()
        mod = unimod_db.get(mod_name)
        if mod is None:
            logger.warning("Unimod ID not found for modification: %s", mod_name)
            return mod_name

        return f"[UNIMOD:{mod.id}]"

    @classmethod
    def _splice_mods(
        cls,
        seq: str,
        mods: Mods,
        *,
        index_base: int = 1,
        wrap: Callable[[str], str] = lambda m: f"[{m}]",
    ):
        """
        Insert each modification string into `seq` AFTER the residue at `location`.

        :param seq: Original peptide sequence, e.g. "ABCDEF".
        :type seq: str
        :param mods: List of modification dicts with keys:
                 - "location" (int)
                 - "residue" (str or None)
                 - "modification" (str)
                 Default indexing is 1-based. Special case (1-based only): location == 0 with residue None inserts at the start.
        :type mods: list[dict]
        :param index_base: Indexing base, either 1 (default) or 0. Positions refer to the ORIGINAL sequence.
        :type index_base: int
        :param wrap: Callable that renders a modification (default: lambda m: f"[{m}]").
        :type wrap: Callable[[str], str]
        :returns: Sequence with modifications spliced in.
        :rtype: str
        :raises ValueError: if index_base is not 0 or 1, if a 1-based location 0 has a residue, or if a provided residue does not match the sequence.
        :raises IndexError: if a 0-based location is negative or any location is out of range for the sequence.
        """
        # Group modifications by normalized 0-based location.
        # Use sentinel -1 for "before the first residue".
        grouped = defaultdict(list)

        for m in mods:
            loc = m["location"]
            res = m.get("residue", None)
            mod = m["name"]

            if index_base == 1:
                if loc == 0:
                    # only allowed when residue is None
                    if res not in (None, ""):
                        raise ValueError(
                            "location 0 must have no residue (use None or omit)."
                        )
                    grouped[-1].append(mod)
                    continue
                loc0 = loc - 1
            elif index_base == 0:
                if loc < 0:
                    raise IndexError("location must be >= 0 for 0-based indexing")
                loc0 = loc
            else:
                raise ValueError("index_base must be 0 or 1")

            if not (0 <= loc0 < len(seq)):
                raise IndexError(
                    f"location {loc} out of range for sequence of length {len(seq)} (index_base={index_base})"
                )

            # Sanity check residue if provided
            if res is not None and res != seq[loc0]:
                raise ValueError(
                    f"Residue mismatch at location {loc}: expected '{seq[loc0]}', got '{res}'"
                )

            grouped[loc0].append(mod)

        out = []
        cursor = 0

        # Emit any beginning-of-sequence mods first (sentinel -1), if present
        if -1 in grouped:
            out.extend(wrap(m) for m in grouped[-1])
            # then fall through to normal stitching

        # Stitch once over the ORIGINAL sequence, inserting AFTER each residue
        for loc0 in sorted(k for k in grouped.keys() if k != -1):
            out.append(seq[cursor : loc0 + 1])  # include the residue at loc0
            out.extend(wrap(m) for m in grouped[loc0])  # then insert its mods
            cursor = loc0 + 1

        out.append(seq[cursor:])  # tail
        return "".join(out)

    @classmethod
    def generate_usis(
        cls, project_accession: str, file_path: Path
    ) -> Generator[str, None, None]:
        for item in mzid.read(source=str(file_path)):
            title: str = item["spectrum title"]
            pattern = re.compile(
                r'File:"(?P<filename>[^"]+)"[\s\S]*?scan=(?P<scan>\d+)'
            )
            m = pattern.search(title)
            if not m:
                logger.warning(
                    "No match for %s with title %s. Skipping entry.",
                    item["spectrumID"],
                    title,
                )
                continue

            filename = Path(m.group("filename")).stem
            scan_number = int(m.group("scan"))

            if len(item["SpectrumIdentificationItem"]) > 1:
                logger.warning(
                    "Multiple SpectrumIdentificationItem for %s", item["spectrumID"]
                )

            seq_obj: dict[str, Any] = item["SpectrumIdentificationItem"][0]
            charge: int = seq_obj["chargeState"]
            seq: str = seq_obj["PeptideSequence"]
            if seq_obj.get("Modification"):
                seq = cls._splice_mods(
                    seq,
                    seq_obj["Modification"],
                    wrap=cls._replace_with_unimod,
                )

            usi = f"mzspec:{project_accession}:{filename}:scan:{scan_number}:{seq}/{charge}"
            yield usi


if __name__ == "__main__":
    SAMPLE_ACCESSION = "PXD001357"
    usigrabber_root = data_directory_path()
    root_path = usigrabber_root / "project_archive"
    project_path = root_path / SAMPLE_ACCESSION

    filepath = project_path / "OTE0019_York_060813_JH16_F119502.mzid"

    counter = 0
    for usi in MZID.generate_usis(SAMPLE_ACCESSION, filepath):
        counter += 1
        print(usi)

        # limit output for testing
        if counter >= 1:
            break
