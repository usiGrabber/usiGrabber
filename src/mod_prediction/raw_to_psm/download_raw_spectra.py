"""Download raw files from PRIDE and extract spectra data using ThermoRawFileParser."""

import argparse
import asyncio
import logging
import logging.handlers
import multiprocessing
import os
from collections.abc import Generator
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from multiprocessing.synchronize import Lock
from pathlib import Path
from typing import cast
from urllib.parse import ParseResult, urlparse

import aioftp
import pandas as pd
import requests
from dotenv import load_dotenv
from requests.models import Response
from tenacity import (
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random_exponential,
)

from mod_prediction.logging_config import setup_logging, worker_log_configurer
from mod_prediction.parquet_utils import aggregate_modifications_per_psm, read_psm_data
from mod_prediction.raw_to_psm.worker import extract_and_export

logger = logging.getLogger("mod-prediction")

DOWNLOAD_SPEED_IN_MBS = int(os.getenv("DOWNLOAD_SPEED_MBPS", 100)) / 8  # 100 Mbps in MB/s
BASE_URL = "https://www.ebi.ac.uk/pride/ws/archive/v3"

DEFAULT_TEMP_DIR: Path = Path.cwd() / "pride_raw_files"


@dataclass
class ParsedFileInfo:
    accession: str
    ms_run: str
    num_scans: int | None = None
    download_time: float | None = None
    extraction_time: float | None = None
    success: bool = False
    error_class: str | None = None
    error_message: str | None = None


# storage for timings
run_infos: dict[str, ParsedFileInfo] = {}
"map `project_accession-ms_run` to `ParsedFileInfo`"


@retry(
    stop=stop_after_attempt(7),
    wait=wait_exponential(multiplier=1, max=60),
    reraise=True,
)
def get_raw_file_info(project_accession: str, filename_filter: str) -> list | None:
    """
    Fetch raw file information from PRIDE API with exponential backoff retry.

    Args:
        project_accession: PRIDE project accession (e.g., PXD015050)
        filename_filter: Optional filter for filename

    Returns:
        List of file information dictionaries for raw files
    """
    url: str = f"{BASE_URL}/projects/{project_accession}/files/all"
    params: dict[str, str] = {"filenameFilter": filename_filter}

    response: Response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    files = response.json()

    # Filter for raw files only
    raw_files = [
        f
        for f in files
        if f.get("fileCategory", {}).get("value") == "RAW" and f.get("publicFileLocations")
    ]

    logger.debug("Found %d raw files for project %s", len(raw_files), project_accession)
    return raw_files


# retry fairly aggressively with large backoff, up to 10 retries
@retry(
    stop=stop_after_attempt(10),
    wait=wait_random_exponential(multiplier=1.7, min=15, max=90),
    retry=retry_if_not_exception_type(ValueError),
    reraise=True,
)
async def ftp_download(ftp_url: str, out_path: Path, *, filesize_bytes: int | None = None) -> float:
    """
    Download a file via FTP with automatic backoff/retrying.

    Args:
        ftp_url: FTP URL (ftp://host/path)
        out_path: Output file path
        filesize_bytes: Expected size of the file in bytes (optional)
    Returns:
        Duration of download in seconds
    """
    if not out_path.parent.exists():
        raise ValueError(f"Output directory does not exist: {out_path.parent}")

    parsed: ParseResult = urlparse(ftp_url)

    if not parsed.hostname or not parsed.path:
        raise ValueError(f"Invalid FTP URL: {ftp_url}")

    # calculate timeout based on file size and download speed
    timeout = 3600  # default to 1 hour timeout per file
    if filesize_bytes is not None:
        estimated_time = filesize_bytes / (DOWNLOAD_SPEED_IN_MBS * 1024 * 1024)
        timeout = int(estimated_time * 2)  # double the estimated time for safety

    logger.debug("Downloading %s via FTP...", out_path.name)
    loop = asyncio.get_event_loop()
    start_time = loop.time()
    async with aioftp.Client.context(
        parsed.hostname or "",
        user=parsed.username or "anonymous",
        password=parsed.password or "anonymous@",
    ) as client:
        await asyncio.wait_for(
            client.download(parsed.path, str(out_path), write_into=True),
            timeout=timeout,  # timeout errors will be retried
        )
    duration = loop.time() - start_time
    logger.debug("Successfully downloaded '%s' in %.2f seconds", out_path.name, duration)

    return duration


def get_file_info(
    ms_run: str,
    project_accession: str,
) -> tuple[str, str, int]:
    """
    Get file information including FTP URL, file name, and file size from PRIDE API.

    Returns:
        tuple of (ftp_url, raw_file_name, filesize)
    """

    # Fetch raw files for this project
    raw_files = get_raw_file_info(project_accession, ms_run + ".raw")
    if not raw_files:
        raise ValueError(f"No raw files found for project {project_accession}")

    if len(raw_files) > 1:
        logger.warning(
            f"Multiple raw files found for {ms_run} in {project_accession}, using the first one"
        )

    raw_file_info = raw_files[0]
    raw_file_name = raw_file_info.get("fileName", "unknown.raw")

    # Get FTP download URL
    public_locations = raw_file_info.get("publicFileLocations", [])
    ftp_url = None
    for location in public_locations:
        if location.get("cvLabel") == "PRIDE" and location.get("name") == "FTP Protocol":
            ftp_url = location.get("value")
            break

    if not ftp_url:
        raise ValueError(f"No FTP URL found for {raw_file_name}")

    filesize = raw_file_info.get("fileSizeBytes", 0)

    return ftp_url, raw_file_name, filesize


async def process_chunk(
    project_accession: str,
    ms_run: str,
    chunk_df: pd.DataFrame,
    output_dir: Path,
    temp_dir: Path,
    pool: ProcessPoolExecutor,
    charge_mismatch_lock: Lock,
    no_validate_charge: bool = False,
    keep_temp_files: bool = False,
    convert_to_mgf: bool = False,
) -> bool:
    """
    Process a chunk of PSMs for a single project/ms_run combination.

    Downloads raw file, extracts spectra for requested scans, and creates enriched PSM parquet file.

    Args:
        project_accession: PRIDE project accession
        ms_run: MS run name
        chunk_df: DataFrame with PSM data for this chunk (must have index_number column)
        output_dir: Directory to save output parquet file
        temp_dir: Temporary directory for downloading raw files
        no_validate_charge: If True, skip charge state validation
        charge_mismatch_lock: Lock for synchronizing charge mismatch file writes
    Returns:
        True if successful, False otherwise
    """
    raw_file_path: Path | None = None

    run_info = ParsedFileInfo(accession=project_accession, ms_run=ms_run)

    try:
        # Get unique scan numbers from chunk
        num_scan_numbers = len(chunk_df)
        scan_numbers = sorted(set(chunk_df["index_number"].tolist()))
        if num_scan_numbers != len(scan_numbers):
            logger.warning(
                "Duplicate scan numbers found in chunk for %s/%s: %d PSMs map to %d unique scans",
                project_accession,
                ms_run,
                num_scan_numbers,
                len(scan_numbers),
            )
        logger.debug("Processing %d unique scans from %d PSMs", len(scan_numbers), len(chunk_df))

        if not scan_numbers:
            raise ValueError(f"No scan numbers provided for {project_accession}/{ms_run}")
        run_info.num_scans = len(scan_numbers)

        ftp_url, raw_file_name, filesize_bytes = get_file_info(ms_run, project_accession)
        raw_file_path = temp_dir / raw_file_name

        # ===== DOWNLOAD =====
        download_duration = 0.0
        if not raw_file_path.exists():
            download_duration = await ftp_download(
                ftp_url, raw_file_path, filesize_bytes=filesize_bytes
            )
        else:
            logger.info("Using cached raw file: %s", raw_file_path)
        run_info.download_time = download_duration

        # ===== EXTRACT =====
        # run in separate process to avoid blocking event loop and to allow parallel processing of multiple files
        extraction_duration = await asyncio.get_running_loop().run_in_executor(
            pool,
            extract_and_export,
            project_accession,
            ms_run,
            raw_file_path,
            scan_numbers,
            output_dir,
            chunk_df,
            charge_mismatch_lock,
            no_validate_charge,
            convert_to_mgf,
        )
        run_info.extraction_time = extraction_duration
        run_info.success = True
    except Exception as e:
        logger.error(f"Error processing chunk for {project_accession}/{ms_run}: {e}")
        run_info.error_class = e.__class__.__name__
        run_info.error_message = str(e)
    finally:
        # Clean up temporary raw file
        if not keep_temp_files and raw_file_path is not None and raw_file_path.exists():
            try:
                raw_file_path.unlink()
                logger.debug("Deleted temporary raw file: %s", raw_file_path)
            except Exception as e:
                logger.warning("Failed to delete temporary raw file %s: %s", raw_file_path, e)

    run_infos[f"{project_accession}-{ms_run}"] = run_info
    return run_info.success


def data_generator(
    input_file: Path,
    limit: int | None = None,
) -> Generator[tuple[str, str, pd.DataFrame], None, None]:
    # reading, grouping and sorting this file may take a while for large input files
    # we save a copy of the aggregated/sorted file for faster retrying

    aggregated_sorted_file = input_file.parent / f"{input_file.stem}_aggregated{input_file.suffix}"

    if aggregated_sorted_file.exists():
        logger.info(f"Using cached aggregated/sorted file: '{aggregated_sorted_file}'")
        input_file = aggregated_sorted_file
        df_aggregated = aggregate_modifications_per_psm(read_psm_data(input_file))
    else:
        logger.info("No pre-aggregated file found, processing input file...")

        # Read and prepare data
        df: pd.DataFrame = read_psm_data(input_file)

        df_aggregated = aggregate_modifications_per_psm(df)
        df_aggregated = df_aggregated.sort_values(by=["project_accession", "ms_run"])
        df_aggregated.to_csv(aggregated_sorted_file, index=False)
        logger.info(f"Saved aggregated/sorted file to '{aggregated_sorted_file}'")

    if limit:
        df_aggregated: pd.DataFrame = df_aggregated.head(limit)
        logger.info(f"Limited to {limit} rows for testing")

    grouped = df_aggregated.groupby(["project_accession", "ms_run"], sort=False)
    group_counts = grouped.size().sort_values(ascending=False)  # type: ignore

    for group_key in group_counts.index:
        project_accession, ms_run = cast(tuple[str, str], group_key)
        chunk_df = cast(pd.DataFrame, grouped.get_group(group_key))

        # skip files with less than 5 PSMs - download/extraction overhead not worth it
        if len(chunk_df) < 5:
            continue
        yield project_accession, ms_run, chunk_df

    logger.info(f"Generated {len(group_counts)} project/ms_run groups from input data")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download raw files from PRIDE and extract spectra with PSM enrichment"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to Parquet or CSV file with PSM data (must have project_accession, ms_run, and index_number columns)",
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        help="Output directory for enriched PSM parquet files (one file per raw file, named: projectAccession_ms_run_name.parquet)",
    )
    parser.add_argument(
        "-y",
        action="store_true",
        help="Overwrite existing output files",
        default=False,
    )
    parser.add_argument(
        "-t",
        "--temp-dir",
        type=Path,
        help=(
            "Temporary directory for downloaded raw files "
            "(default: stored in a temporary directory inside output_dir)"
        ),
    )
    parser.add_argument(
        "-l",
        "--limit",
        type=int,
        default=None,
        help="Limit number of rows to process (for testing)",
    )
    parser.add_argument(
        "-k",
        "--keep-temp-files",
        action="store_true",
        help="Keep downloaded raw files and extraction directories after processing",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip charge state validation between PSM and extracted spectrum",
        default=False,
    )
    parser.add_argument(
        "-d",
        "--parallel-downloads",
        type=int,
        default=4,
        help="Maximum number of concurrent downloads (default: 4)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=8,
        help="Number of parallel workers for extracting files (default: 8)",
    )
    parser.add_argument(
        "--convert-to-mgf",
        action="store_true",
        help=(
            "After processing, convert all enriched PSM parquet files to a single MGF file. "
            "WARNING: This may take a while!"
        ),
        default=False,
    )

    args = parser.parse_args()

    # argument validation
    if not args.input_file.exists():
        raise FileNotFoundError(f"Input file not found: '{args.input_file}'")

    if not args.output_dir.exists():
        raise FileNotFoundError(f"Output directory not found: '{args.output_dir}'")

    # Create directories
    if args.temp_dir:
        args.temp_dir.mkdir(parents=True, exist_ok=True)

    return args


async def _async_main() -> None:
    """Async implementation of main."""

    load_dotenv()

    args = parse_args()

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_dir = args.output_dir / timestamp
    run_dir.mkdir(exist_ok=False)

    # temporary folder to store the raw files for this run
    tmp_dir = args.temp_dir or run_dir / "temp_raw_files"
    tmp_dir.mkdir(exist_ok=True)

    # location to store the parquet output files for this run
    parquet_output_dir = run_dir / "output"
    parquet_output_dir.mkdir(exist_ok=False)

    log_dir = run_dir / "logs"
    setup_logging(log_dir)

    logger.info(f"Input file: '{args.input_file}'")
    logger.info(f"Output directory: '{run_dir}'")
    logger.info(f"Parallel downloads: {args.parallel_downloads}")
    logger.info(f"Concurrent workers: {args.workers}")

    logger.info("Starting raw file download, spectra extraction, and PSM enrichment...")

    start_time = asyncio.get_event_loop().time()

    # since it is only released after the parsing is complete, this limits overall concurrency
    # TODO: refactor to release once download has finished
    sem = asyncio.Semaphore(args.parallel_downloads)

    # We use a Manager Queue because it handles shared memory across processes cleanly
    with multiprocessing.Manager() as m:
        log_queue = m.Queue()
        charge_mismatch_lock = m.Lock()

        successfully_processed = 0

        # We need to keep references to running tasks to avoid garbage collection
        # and to wait for the final batch to finish.
        background_tasks = set()

        def _finish_callback(task: asyncio.Task) -> None:
            nonlocal successfully_processed

            try:
                # Safely obtain the task result without propagating exceptions
                result = task.result()
            except Exception as exc:
                logger.error("Background task failed: %s", exc)
            else:
                if result:
                    successfully_processed += 1
            finally:
                # Always release semaphore and discard the task, even on failure
                sem.release()
                background_tasks.discard(task)

        with ProcessPoolExecutor(
            max_workers=args.workers, initializer=worker_log_configurer, initargs=(log_queue,)
        ) as pool:
            # This thread runs in the main process, pulling logs from the queue
            # and writing them to your elaborate handlers.
            root_logger = logging.getLogger()
            listener = logging.handlers.QueueListener(
                log_queue, *root_logger.handlers, respect_handler_level=True
            )
            listener.start()

            try:
                # Process each project/ms_run combination
                for project_accession, ms_run, chunk_df in data_generator(
                    args.input_file,
                    args.limit,
                ):
                    # wait for free worker
                    await sem.acquire()

                    try:
                        logger.info(
                            f"Processing '{project_accession}/{ms_run}' with {len(chunk_df)} PSMs"
                        )

                        # spawn the task
                        task = asyncio.create_task(
                            process_chunk(
                                project_accession,
                                ms_run,
                                chunk_df,
                                parquet_output_dir,
                                tmp_dir,
                                pool,
                                charge_mismatch_lock=charge_mismatch_lock,  # type: ignore
                                no_validate_charge=args.no_validate,
                                keep_temp_files=args.keep_temp_files,
                                convert_to_mgf=args.convert_to_mgf,
                            )
                        )
                    except Exception:
                        sem.release()
                        raise
                    else:
                        # add to set and remove when done
                        background_tasks.add(task)
                        task.add_done_callback(_finish_callback)

                # wait for the stragglers (the final batch still running)
                if background_tasks:
                    await asyncio.gather(*background_tasks, return_exceptions=True)
            except AttributeError as e:
                if "'util'" in str(e):
                    logger.error("weird but expected multiprocessing error: " + str(e))
                else:
                    raise
            finally:
                listener.stop()

                # Save timings
                timings_path = run_dir / "result.csv"
                logger.info(f"Saving timings to {str(timings_path)}")
                timings = pd.DataFrame(
                    [asdict(info) for info in run_infos.values()],
                    columns=pd.Index(
                        [
                            "accession",
                            "ms_run",
                            "num_scans",
                            "download_time",
                            "extraction_time",
                            "success",
                            "error_class",
                            "error_message",
                        ]
                    ),
                )
                timings.to_csv(timings_path, index=False)

                total_duration = asyncio.get_event_loop().time() - start_time
                total_scans = sum(
                    t.num_scans for t in run_infos.values() if t.num_scans is not None and t.success
                )

                logger.info(
                    "Successfully processed %d project/ms_run combinations with %d scans in %s (%.2f scans/second)",
                    successfully_processed,
                    total_scans,
                    str(timedelta(seconds=total_duration)),
                    total_scans / total_duration if total_duration > 0 else 0,
                )


def main() -> None:
    """CLI entry point."""
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
