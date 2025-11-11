import asyncio
import gzip
import os
import shutil
import tarfile
import tempfile
import zipfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aioftp
import typer

from usigrabber.utils import DATA_DIR, logger


async def download_ftp(
    url: str,
    out_dir: Path,
    file_name: str | None = None,
    retries: int = 3,
    delay: int = 5,
) -> Path | None:
    parsed = urlparse(url)
    if parsed.scheme != "ftp":
        raise ValueError(f"URL scheme for {url} is not FTP, found {parsed.scheme}")
    filename = file_name or os.path.basename(parsed.path)
    out_path = out_dir / filename
    out_dir.mkdir(parents=True, exist_ok=True)

    for attempt in range(retries):
        try:
            async with aioftp.Client.context(
                parsed.hostname,
                user=parsed.username or "anonymous",
                password=parsed.password or "anonymous@",
            ) as client:
                await client.download(parsed.path, str(out_path))
            return out_path
        except Exception:
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                return None


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
    out_path = asyncio.run(download_ftp(url, out_dir, file_name=filename))

    if extract:
        extract_archive(out_path, out_dir)


if __name__ == "__main__":
    typer.run(main)
