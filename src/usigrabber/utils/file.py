import asyncio
import gzip
import logging
import ntpath
import os
import posixpath
import shutil
import tarfile
import tempfile
import zipfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any
from urllib.parse import urlparse

import aioftp
import typer
from async_http_client import AsyncHttpClient

logger = logging.getLogger(__name__)


async def download_ftp(
    url: str,
    out_dir: Path,
    file_name: str | None = None,
    retries: int = 3,
    delay: int = 5,
) -> Path:
    """Download a file from an FTP or HTTP URL asynchronously.

    For PRIDE HTTP URLs (https://ftp.pride.ebi.ac.uk/...), uses AsyncHttpClient.
    For other FTP URLs, falls back to aioftp client.
    """

    logger.debug("Downloading file from '%s'", url)

    parsed = urlparse(url)
    filename = file_name or os.path.basename(parsed.path)
    out_path = out_dir / filename
    out_dir.mkdir(parents=True, exist_ok=True)

    # Use HTTP client for PRIDE HTTP URLs
    if parsed.scheme in ("http", "https") and "ftp.pride.ebi.ac.uk" in url:
        logger.debug("Using HTTP client for PRIDE URL: %s", url)
        async with AsyncHttpClient(retry_attempts=retries, verbose=False) as client:
            downloaded_path = await client.stream_file(url, download_file_name=out_path)
            return downloaded_path

    # Fall back to FTP client for other FTP URLs
    if parsed.scheme == "ftp":
        logger.debug("Using FTP client for URL: %s", url)
        assert parsed.hostname is not None, "FTP URL must have a hostname"

        for attempt in range(retries):
            try:
                async with aioftp.Client.context(
                    parsed.hostname,
                    user=parsed.username or "anonymous",
                    password=parsed.password or "anonymous@",
                ) as client:
                    await client.download(parsed.path, str(out_path), write_into=True)
                return out_path
            except ConnectionResetError:
                if attempt > 2:
                    # first few attempts often fail, so only log after 2nd attempt
                    logger.warning(
                        f"FTP connection reset on attempt {attempt + 1}/{retries} for {url}",
                    )
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
                else:
                    raise
            except Exception:
                if attempt > 2:
                    logger.warning(
                        f"FTP download attempt {attempt + 1}/{retries} failed for {url}",
                        exc_info=True,
                    )
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
                else:
                    raise

        raise RuntimeError("Unreachable: retry loop ended without returning or raising")

    raise ValueError(f"Unsupported URL scheme for {url}: {parsed.scheme}")


async def download_ftp_with_semamphore(
    semaphore: asyncio.Semaphore,
    url: str,
    out_dir: Path,
) -> Path:
    """Download a file from an FTP URL asynchronously with a semaphore."""
    async with semaphore:
        return await download_ftp(url, out_dir)


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
    archive_path = archive_path.resolve()
    extract_to = extract_to.resolve()

    extract_to.mkdir(parents=True, exist_ok=False)

    # --- CRITICAL FIX: directory named *.gz, *.zip, etc ---
    if archive_path.is_dir():
        # Not a real archive; return directory contents but no extraction
        return [p for p in archive_path.iterdir()]

    # Not an archive → return as-is
    if not is_archive_file(archive_path):
        return [archive_path]

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


@contextmanager
def temporary_path(*, suffix="", prefix="tmp", dir=None) -> Generator[Path, Any, None]:
    with tempfile.TemporaryDirectory(suffix=suffix, prefix=prefix, dir=dir) as tmpdir:
        yield Path(tmpdir)


def is_windows_path(raw) -> bool:
    """
    Detect if a given path is a Windows path.
    Credit: https://stackoverflow.com/a/79816962/7432003
    """
    return len(PureWindowsPath(raw).parts) > len(PurePosixPath(raw).parts)


def parse_basename(raw_path: str) -> str:
    """
    Standard `os.path.basename` only supports the current OS running the program.
    This function uses the underlying Windows/POSIX function to parse the basename correctly.
    """
    if is_windows_path(raw_path):
        return ntpath.basename(raw_path)
    return posixpath.basename(raw_path)


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
