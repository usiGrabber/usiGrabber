import gzip
import logging
import os
import shutil
import tarfile
import tempfile
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable, Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import typer
from tqdm import tqdm

from usigrabber.utils import DATA_DIR, logger


def download_ftp(url: str, out_dir: Path, file_name: str | None = None) -> Path:
    parsed = urllib.parse.urlparse(url)
    filename = file_name or os.path.basename(parsed.path)
    out_path = out_dir / filename

    if logger.level > logging.DEBUG:
        urllib.request.urlretrieve(url, filename=str(out_path))

    # Build a reporthook that updates tqdm
    def _tqdm_hook(t: tqdm) -> Callable[[int, int, int | None], None]:
        last = [0]

        def inner(blocks: int = 1, block_size: int = 1, total_size: int | None = None) -> None:
            if total_size is not None and total_size > 0 and t.total is None:
                t.total = total_size  # set total once we learn it
            downloaded = blocks * block_size
            t.update(downloaded - last[0])
            last[0] = downloaded

        return inner

    with tqdm(
        total=None,  # will be filled if server reports SIZE
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=filename,
    ) as t:
        # urlretrieve handles ftp://… and calls our hook periodically
        urllib.request.urlretrieve(url, filename=str(out_path), reporthook=_tqdm_hook(t))

    return out_path


def extract_archive(archive_path: Path, extract_to: Path) -> None:
    # extracts all files/folders from archive directly to extract_to
    archive_str = str(archive_path)
    members = []

    if not archive_str.endswith((".zip", ".tar", ".tar.gz", ".tgz", ".gz")):
        logger.debug("No extraction needed for path: %s", archive_path)
        return
    else:
        os.makedirs(extract_to, exist_ok=True)

    if archive_str.endswith(".zip"):
        with zipfile.ZipFile(archive_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
        members = zip_ref.namelist()
    elif archive_str.endswith((".tar", ".tar.gz", ".tgz")):
        with tarfile.open(archive_path, "r:*") as tar_ref:
            members = tar_ref.getnames()
            tar_ref.extractall(extract_to)
    elif archive_str.endswith(".gz"):
        output_file = os.path.join(extract_to, os.path.basename(str(archive_path)[:-3]))
        with gzip.open(archive_path, "rb") as f_in, open(output_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        members = [os.path.basename(output_file)]
    else:
        logger.debug("Unsupported archive format for path: %s", archive_path)
        return

    for member in members:
        extract_archive(extract_to / Path(member), extract_to / os.path.splitext(member)[0])

    logger.debug("Extracted %s to %s", archive_path, extract_to)


@contextmanager
def temporary_path(*, suffix="", prefix="tmp", dir=None) -> Generator[Path, Any, None]:
    with tempfile.TemporaryDirectory(suffix=suffix, prefix=prefix, dir=dir) as tmpdir:
        yield Path(tmpdir)


def main(
    url: str,
    out_dir: Path = DATA_DIR / "files",
    filename: str | None = None,
    extract: bool = False,
) -> None:
    out_path = download_ftp(url, out_dir, file_name=filename)

    if extract:
        extract_archive(out_path, out_dir)


if __name__ == "__main__":
    typer.run(main)
