import asyncio
import gzip
import logging
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

from usigrabber.backends.base import FileMetadata

logger = logging.getLogger(__name__)

# empty string for folders (no extension)
FILETYPE_WHITELIST = {".mzid", "", ".txt"}
ARCHIVE_TYPES = {".zip", ".gz", ".tar", ".rar", ".7z"}


async def download_ftp(
    url: str,
    out_dir: Path,
    file_name: str | None = None,
    retries: int = 3,
    delay: int = 5,
) -> Path:
    """Download a file from an FTP URL asynchronously."""

    logger.debug("Downloading FTP file from '%s'", url)

    parsed = urlparse(url)
    if parsed.scheme != "ftp":
        raise ValueError(f"URL scheme for {url} is not FTP, found {parsed.scheme}")
    assert parsed.hostname is not None, "FTP URL must have a hostname"
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
                await client.download(parsed.path, str(out_path), write_into=True)
            return out_path
        except Exception:
            logger.warning(
                f"FTP download attempt {attempt + 1}/{retries} failed for {url}",
                exc_info=True,
            )
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                raise

    raise RuntimeError("Unreachable: retry loop ended without returning or raising")


def is_archive_file(path: Path) -> bool:
    """Return True only if this is an actual file archive, not a directory."""
    if path.is_dir():
        return False  # directories are never archives

    p = str(path).lower()

    # .tar.gz or .tgz
    if p.endswith((".tar.gz", ".tgz")):
        return True

    # .zip or .tar
    if p.endswith((".zip", ".tar")):
        return True

    # single-file compression (NOT .tar.gz)
    return bool(p.endswith(".gz") and not p.endswith(".tar.gz"))


def extract_archive(
    archive_path: Path,
    extract_to: Path,
) -> list[Path]:
    """
    Recursively extract archives into extract_to.
    Returns a list of extracted file paths.
    """
    # Not an archive → return as-is
    if not is_archive_file(archive_path):
        return [archive_path]

    archive_path = archive_path.resolve()

    extract_to.mkdir(parents=True, exist_ok=False)

    # --- CRITICAL FIX: directory named *.gz, *.zip, etc ---
    if archive_path.is_dir():
        # Not a real archive; return directory contents but no extraction
        return [p for p in archive_path.iterdir()]

    archive_str = str(archive_path).lower()

    # --- ZIP ---
    if archive_str.endswith(".zip"):
        with zipfile.ZipFile(archive_path, "r") as z:
            z.extractall(extract_to)
            members = z.namelist()

    # --- TAR / TAR.GZ / TGZ ---
    elif archive_str.endswith((".tar", ".tar.gz", ".tgz")):
        with tarfile.open(archive_path, "r:*") as t:
            members = t.getnames()
            t.extractall(extract_to)

    # --- Single-file GZIP (e.g. file.mzid.gz, file.txt.gz) ---
    elif archive_str.endswith(".gz"):
        output_name = archive_path.stem  # remove only .gz
        output_path = extract_to / output_name

        with gzip.open(archive_path, "rb") as f_in, open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

        members = [output_name]

    else:
        # unknown format (should never reach here)
        return [archive_path]

    # ------------------------------------------------------
    # Handle extracted members (recursively if archives)
    # ------------------------------------------------------
    extracted_members: list[Path] = []
    for m in members:
        member_path = (extract_to / m).resolve()

        # Security check: ensure member is within extract_to
        if not member_path.is_relative_to(extract_to):
            logger.warning(
                "Skipping member with path outside extraction directory: %s", member_path
            )
            continue

        # Skip directories
        if member_path.is_dir():
            extracted_members.append(member_path)
            continue

        # Recurse only into REAL archive files
        if is_archive_file(member_path):
            next_extract_dir = extract_to / member_path.stem
            extracted_members.extend(extract_archive(member_path, next_extract_dir))
        else:
            extracted_members.append(member_path)

    return extracted_members


async def get_interesting_files(
    files: list[FileMetadata], accession: str, tmp_dir: Path
) -> dict[str, list[Path]]:
    interesting_files: dict[str, list[Path]] = {ext: [] for ext in FILETYPE_WHITELIST}
    for file in files:
        file_url = file["filepath"]
        filename = os.path.basename(file_url)

        # find actual file extension, without archives
        file_base, file_ext = os.path.splitext(filename)
        while file_ext in ARCHIVE_TYPES:
            file_base, file_ext = os.path.splitext(file_base)

        if (file_ext not in FILETYPE_WHITELIST) and ("txt.zip" not in str(filename)):
            logger.debug(f"Skipping file {filename} with unsupported extension {file_ext}.")
            continue
        if file_ext == ".txt" and file_base not in ("evidence", "summary", "peptides"):
            logger.debug(f"Skipping file {filename} as it is not interesting.")
            continue

        logger.debug(
            f"Processing {file['category']} file {filename} "
            + f"({file['file_size'] / (1024 * 1024):,.2f} MB)"
        )

        try:
            path = await download_ftp(
                url=file_url,
                out_dir=tmp_dir,
                file_name=filename,
            )
        except Exception:
            logger.error(
                "Failed to download file %s for project %s.",
                filename,
                accession,
                exc_info=True,
            )
            continue

        # contains all files extracted from archive
        extracted_files = extract_archive(archive_path=path, extract_to=tmp_dir / "extracted")

        for f in extracted_files:
            ext = os.path.splitext(str(f))[1]
            if ext in FILETYPE_WHITELIST:
                interesting_files[ext].append(f)
    return interesting_files


@contextmanager
def temporary_path(*, suffix="", prefix="tmp", dir=None) -> Generator[Path, Any, None]:
    with tempfile.TemporaryDirectory(suffix=suffix, prefix=prefix, dir=dir) as tmpdir:
        yield Path(tmpdir)


def main(
    url: str,
    out_dir: Path = Path("."),
    filename: str | None = None,
    extract: bool = False,
) -> None:
    out_path = asyncio.run(download_ftp(url, out_dir, file_name=filename))

    if extract and out_path:
        extract_archive(out_path, out_dir)


if __name__ == "__main__":
    typer.run(main)
