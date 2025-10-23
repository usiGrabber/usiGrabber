import os
import tarfile
import urllib.parse
import urllib.request
from pathlib import Path

from usigrabber.utils import logger


def download_ftp(url: str, out_dir: Path, file_name: str | None = None) -> Path:
    # create directory
    out_dir.mkdir(parents=True)

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
                f"\rDownloading {filename}: {pct:5.1f}% ({downloaded_mb:5.1f}MB/{total_mb:5.1f}MB)",
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
    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(path=extract_to)
    logger.debug("Extracted %s to %s", archive_path, extract_to)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Download a file from an FTP URL and optionally extract it."
    )
    parser.add_argument("url", help="FTP URL to download")
    parser.add_argument(
        "-o",
        "--out-dir",
        default="downloads",
        help="Output directory for the downloaded file (default: ./downloads)",
    )
    parser.add_argument(
        "-f",
        "--filepath",
        default=None,
        help="Optional filename to save as (defaults to the name from the URL)",
    )
    parser.add_argument(
        "-x",
        "--extract",
        action="store_true",
        help="If set, automatically extract the downloaded .tar.gz archive to the output directory",
    )

    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    file_name = args.filepath

    out_path = download_ftp(args.url, out_dir, file_name)

    if args.extract:
        extract_archive(out_path, out_dir)
