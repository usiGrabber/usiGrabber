import hashlib
from collections.abc import Sequence
from pathlib import Path


def md5_checksum(file_path: Path | Sequence[Path]) -> str:
    file_paths = list(file_path) if isinstance(file_path, Sequence) else [file_path]
    file_paths = sorted(file_paths)

    hash_md5 = hashlib.md5(usedforsecurity=False)
    for file in file_paths:
        with open(file, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
    return hash_md5.hexdigest()
