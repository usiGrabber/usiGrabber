import asyncio
import csv
import multiprocessing
import os
import random
import shutil
from pathlib import Path

import ijson
import matplotlib.pyplot as plt
import pandas as pd
from pyteomics import mzid
from tqdm import tqdm

from usigrabber.file_parser.mzid.parsing_functions import parse_spectra_data
from usigrabber.utils import get_cache_dir
from usigrabber.utils.file import download_ftp_with_semamphore, extract_archive
from usigrabber.utils.setup import system_setup

ALL_MZID_PATH = get_cache_dir() / "pride" / "all_mzid_files.csv"
SAMPLED_MZID_PATH = get_cache_dir() / "pride" / "sampled_mzid_files.csv"
SAMPLED_MZID_DIR = get_cache_dir() / "pride" / "sampled_mzid_files"


def sample_mzid_files(n: int = 250) -> None:
    all_files_path = get_cache_dir() / "pride" / "all_files.json"

    mzid_file_counter = 0

    with open(ALL_MZID_PATH, "w", newline="") as f, open(all_files_path, "rb") as in_f:
        writer = csv.writer(f)

        # write header
        writer.writerow(["filename", "ftp_location", "file_size_bytes", "project_accession"])

        for item in ijson.items(in_f, "item"):
            project_accessions: list[str] = item.get("projectAccessions", [])
            assert len(project_accessions) == 1, "Expected single project accession"
            project_accession = project_accessions[0]

            locations = item.get("publicFileLocations", [])
            location: str | None = next(
                (loc.get("value") for loc in locations if loc.get("name", "").startswith("FTP")),
                None,
            )
            assert location is not None, "FTP location not found"

            filename = item.get("fileName", "")

            # parse file extension from url
            basename, ext = os.path.splitext(filename)
            if ext == ".mzid" or os.path.splitext(basename)[1] == ".mzid":
                writer.writerow(
                    (filename, location, item.get("fileSizeBytes", 0), project_accession)
                )
                mzid_file_counter += 1

    if mzid_file_counter == 0:
        print("No mzid files to sample")
        return

    print(f"Found {mzid_file_counter} mzid files in PRIDE")

    # get random sample indices
    sample_indices = set(random.sample(range(mzid_file_counter), n))

    with (
        open(ALL_MZID_PATH, newline="", encoding="utf-8") as infile,
        open(SAMPLED_MZID_PATH, "w", newline="", encoding="utf-8") as outfile,
    ):
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # copy header
        header = next(reader, None)
        if header:
            writer.writerow(header)

        # write only sampled rows (indexes correspond to data rows, 0-based)
        for idx, row in enumerate(reader):
            if idx in sample_indices:
                writer.writerow(row)

    print(f"Wrote {n} sampled mzid files to {SAMPLED_MZID_PATH}")


def basic_stats() -> None:
    df = pd.read_csv(ALL_MZID_PATH)

    total_size_bytes = df["file_size_bytes"].sum()
    total_size_gb = total_size_bytes / (1024**3)

    print(f"Total mzid files: {len(df)}")
    print(f"Total size (GB): {total_size_gb:.2f}")

    mzids_by_project = df.groupby("project_accession").size()
    print(f"Total projects with mzid files: {len(mzids_by_project)}")

    # average number of mzid files per project where project has at least one mzid file
    avg_mzids_per_project = mzids_by_project.mean()
    print(f"Average mzid files per project: {avg_mzids_per_project:.2f}")

    # convert sizes to megabytes and round to 2 decimal places for readability
    size_mb = (df["file_size_bytes"].fillna(0) / (1024**2)).round(2)

    # define reasonable bins (MB) to make the distribution feasible to view
    bins = [0, 0.01, 0.1, 1, 10, 100, 1_000, 100_000]  # MB
    labels = [
        "<=0.01MB",
        "0.01-0.1MB",
        "0.1-1MB",
        "1-10MB",
        "10-100MB",
        "100-1000MB",
        "1000-100000MB",
    ]

    # categorize and count
    categories = pd.cut(size_mb, bins=bins, labels=labels, include_lowest=True, right=False)
    counts = categories.value_counts().reindex(labels).fillna(0).astype(int)

    # plot
    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(labels, counts, color="#4C72B0")
    ax.set_xlabel("File size (MB)")
    ax.set_ylabel("Number of mzid files")
    ax.set_title("Distribution of mzid file sizes")
    # set explicit tick positions before setting labels to avoid UserWarning
    tick_positions = [bar.get_x() + bar.get_width() / 2 for bar in bars]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(labels, rotation=45, ha="right")

    # annotate counts on bars
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax.annotate(
                str(int(height)),
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=8,
            )

    plt.tight_layout()

    # ensure output directory exists and save the figure
    out_path = get_cache_dir() / "pride" / "mzid_size_distribution.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150)
    print(f"Saved mzid size distribution plot to {out_path.absolute()}")

    # print 10 largest files and their sizes
    largest_files = df.nlargest(10, "file_size_bytes")[["filename", "file_size_bytes"]]
    print("10 largest mzid files:")
    for _, row in largest_files.iterrows():
        size_gb = row["file_size_bytes"] / (1024**3)
        print(f"\t- {row['filename']}: {size_gb:,.2f} GiB")


async def download_samples() -> None:
    semaphore = asyncio.Semaphore(20)  # limit to 5 concurrent downloads
    df = pd.read_csv(SAMPLED_MZID_PATH)

    if SAMPLED_MZID_DIR.exists():
        while True:
            resp = (
                input(
                    f"Output directory {SAMPLED_MZID_DIR} already exists. "
                    "Do you want to delete it and re-download files? (y/n): "
                )
                .strip()
                .lower()
            )
            if resp == "y":
                shutil.rmtree(SAMPLED_MZID_DIR)
                break
            elif resp == "n":
                print("Aborting download.")
                return

    SAMPLED_MZID_DIR.mkdir(parents=True, exist_ok=False)

    sample_paths: list[str] = df["ftp_location"].tolist()

    download_tasks = [
        download_ftp_with_semamphore(semaphore, sample_path, SAMPLED_MZID_DIR)
        for sample_path in sample_paths
    ]

    # download all files concurrently
    paths = await asyncio.gather(*download_tasks, return_exceptions=True)

    # iterate over files and extract archives
    mzid_paths: list[Path] = []
    for idx, path in enumerate(paths):
        if isinstance(path, BaseException):
            print(f"Error downloading file '{sample_paths[idx]}': {path}")
            continue

        # print(f"Downloaded file to {path}")

        if path.suffix.lower() == ".mzid":
            # no extraction needed
            mzid_paths.append(path)
            continue
        elif path.suffix.lower() in {".gz", ".zip", ".tar", ".tgz"}:
            extracted_files = extract_archive(path, path.parent / path.stem)
            if len(extracted_files) != 1:
                print(
                    f"Warning: extracted {len(extracted_files)} files "
                    f"from {path}, expected single mzid file."
                )
            else:
                mzid_paths.append(extracted_files[0])

            # remove the original archive to save space
            path.unlink()
        else:
            path.unlink()  # remove the original archive
            print(f"Unknown file type for {path}, skipping extraction.")


def sample_stats() -> None:
    df = pd.read_csv(SAMPLED_MZID_PATH)
    total_size_bytes = df["file_size_bytes"].sum()
    total_size_gb = total_size_bytes / (1024**3)
    print(f"Total sampled mzid files: {len(df)} - Total size (GB): {total_size_gb:.2f}")

    # find 10 largest files
    largest_files = df.nlargest(10, "file_size_bytes")[["filename", "file_size_bytes"]]
    print("10 largest sampled mzid files:")
    for _, row in largest_files.iterrows():
        size_gb = row["file_size_bytes"] / (1024**3)
        print(f"\t- {row['filename']}: {size_gb:,.2f} GiB")


PSM_COLUMNS = ["spectrumID", "scan number(s)", "spectrum title", "name", "location"]


def _analyze_single_mzid(mzid_file: Path) -> tuple[bool, list | str]:
    """Analyze a single mzid file and return success status and result.

    Returns:
        tuple: (success: bool, result: list of values or error message)
    """
    try:
        psm = next(mzid.read(str(mzid_file), retrieve_refs=False))
        spectra_data = parse_spectra_data(mzid_file)

        row = [
            mzid_file.name,
            str(mzid_file),
        ]
        for col in PSM_COLUMNS[:-1]:
            row.append(psm.get(col))

        # location
        spectra_ref: str | None = psm.get("spectraData_ref")
        if spectra_data and spectra_ref and spectra_data.get(spectra_ref):
            row.append(spectra_data[spectra_ref][0])
        else:
            row.append(None)
        return (True, row)
    except KeyError as e:
        return (False, f"KeyError reading PSM from {mzid_file.name}: {e}")
    except Exception as e:
        return (False, f"Error reading {mzid_file.name}: {e}")


def analyze_samples() -> None:
    # collect all mzid files (skip directories)
    mzid_files = [f for f in SAMPLED_MZID_DIR.rglob("*.mzid") if f.is_file()]

    if not mzid_files:
        print("No mzid files found to analyze")
        return

    print(f"Found {len(mzid_files)} mzid files to analyze")

    # determine number of processes (use all available cores)
    num_processes = 8  # multiprocessing.cpu_count()
    print(f"Using {num_processes} processes")

    # create output file and write header
    output_path = SAMPLED_MZID_PATH.parent / "mzid_analysis_results.csv"

    with open(output_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(
            [
                "filename",
                "file_path",
            ]
            + PSM_COLUMNS
        )

        # process files in parallel and write results as they complete
        success_count = 0
        error_count = 0

        flush_every = 10  # flush output every N writes
        flush_counter = 0

        with multiprocessing.Pool(processes=num_processes) as pool:
            # Use imap_unordered to process results as they complete
            for success, result in tqdm(
                pool.imap_unordered(_analyze_single_mzid, mzid_files), total=len(mzid_files)
            ):
                if success:
                    writer.writerow(result)
                    flush_counter += 1
                    if flush_counter >= flush_every:
                        out_f.flush()
                        flush_counter = 0

                    success_count += 1
                    # print(f"Wrote analysis for {result[0]}")
                else:
                    error_count += 1
                    tqdm.write(f"Error: {result}")

        print(f"\nAnalysis complete: {success_count} successful, {error_count} errors")
        print(f"Results written to {output_path}")


async def main() -> None:
    # sample_mzid_files()
    # sample_stats()
    # basic_stats()

    # await download_samples()

    analyze_samples()


if __name__ == "__main__":
    system_setup()
    asyncio.run(main())
