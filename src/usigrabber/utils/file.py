import gzip
import os
import shutil
import tarfile
import tempfile
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Generator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import typer

from usigrabber.utils import DATA_DIR, logger


def download_ftp(url: str, out_dir: Path, file_name: str | None = None) -> Path:
    # create directory
    # out_dir.mkdir(parents=True, exist_ok=True)

    parsed = urllib.parse.urlparse(url)
    filename = file_name or os.path.basename(parsed.path)
    out_path = out_dir / filename

    def _reporthook(block_num, block_size, total_size):
        if total_size > 0:
            downloaded = block_num * block_size
            pct = downloaded / total_size * 100
            downloaded = min(downloaded, total_size)
            downloaded_mb = downloaded / (1024 * 1024)
            total_mb = total_size / (1024 * 1024)
            print(
                f"\rDownloading {filename}: {pct:5.1f}% "
                + f"({downloaded_mb:5.1f}MB/{total_mb:5.1f}MB)",
                end="",
            )
        else:
            downloaded_mb = (block_num * block_size) / (1024 * 1024)
            print(f"\rDownloading {filename}: {downloaded_mb:5.1f}MB", end="")

    urllib.request.urlretrieve(url, filename=str(out_path), reporthook=_reporthook)
    print("\n")
    logger.debug("Saved to %s", out_path)
    return out_path


def extract_archive(archive_path: Path, extract_to: Path):
    # extracts all files/folders from archive directly to extract_to
    archive_name, archive_ext = os.path.splitext(str(archive_path))

    if archive_ext == ".zip":
        with zipfile.ZipFile(archive_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)

    elif archive_ext in (".tar", ".tar.gz", ".tgz"):
        with tarfile.open(archive_path, "r:*") as tar_ref:
            tar_ref.extractall(extract_to)

    elif archive_ext == ".gz":
        output_file = os.path.join(extract_to, os.path.basename(str(archive_path)[:-3]))
        with gzip.open(archive_path, "rb") as f_in, open(output_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    else:
        logger.debug(f"Unsupported archive format: {archive_ext}")
        return

    logger.debug(f"Extracted {archive_path} to {extract_to}")


@contextmanager
def temporary_path(*, suffix="", prefix="tmp", dir=None) -> Generator[Path, Any, None]:
    with tempfile.NamedTemporaryFile(
        delete=False, suffix=suffix, prefix=prefix, dir=dir
    ) as f:
        path = Path(f.name)
    try:
        yield path
    finally:
        with suppress(FileNotFoundError):
            path.unlink()


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
