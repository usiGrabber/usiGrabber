
"""PROXI API interaction for downloading spectra."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyteomics.usi import USI, proxi
from tenacity import (
    RetryError,
    retry,
    stop_after_attempt,
    wait_exponential,
)

from mod_prediction.models import PyteomicsSpectrum

logger = logging.getLogger(__name__)
failure_logger = logging.getLogger(f"{__name__}.failures")


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2),
    reraise=True,
)
def _download_spectrum_with_retry(usi: USI) -> PyteomicsSpectrum:
    """
    Download single spectrum from PROXI API (internal function with retry logic).

    Args:
        usi: Universal Spectrum Identifier

    Returns:
        Validated spectrum

    Raises:
        Exception: If download/validation fails after all retries
    """
    # Download from PROXI API
    spectrum_data = proxi(usi, backend="pride")

    # Validate response format
    if not isinstance(spectrum_data, dict):
        raise ValueError(f"Invalid response format for {usi}: got {type(spectrum_data).__name__}")

    return PyteomicsSpectrum.model_validate(spectrum_data)


def download_spectrum(usi: USI) -> PyteomicsSpectrum | None:
    """
    Download single spectrum from PROXI API with exponential backoff retry.

    Args:
        usi: Universal Spectrum Identifier

    Returns:
        Validated spectrum or None if download/validation fails after all retries
    """
    try:
        return _download_spectrum_with_retry(usi)
    except RetryError as e:
        logger.warning(
            f"All 5 retries exhausted for {usi}: {e.last_attempt.exception()}"
        )
        return None
    except Exception as e:
        logger.warning(f"Failed to download spectrum {usi}: {e}")
        return None


def download_spectra(
    usi_list: list[USI],
    failed_files: set[tuple[str, str]] | None = None,
    max_workers: int = 5,
) -> list[tuple[USI, PyteomicsSpectrum]]:
    """
    Download multiple spectra from PROXI API using parallel threads.

    Args:
            usi_list: List of USIs to download
            failed_files: Set of (project_accession, ms_run) tuples to skip.
                         Modified in-place to add newly failed files.
            max_workers: Number of parallel download threads (default: 5)

    Returns:
            List of tuples (USI, spectrum) for successfully downloaded spectra (failures are skipped)
    """
    if failed_files is None:
        failed_files = set()

    if not usi_list:
        logger.warning("Empty USI list provided")
        return []

    logger.info(f"Downloading {len(usi_list)} spectra from PROXI API with {max_workers} workers...")

    # Lock for thread-safe access to failed_files
    failed_files_lock = threading.Lock()

    spectra: list[tuple[USI, PyteomicsSpectrum]] = []
    failures = 0
    skipped = 0

    def download_single(usi: USI) -> tuple[USI, PyteomicsSpectrum | None, bool]:
        """Download a single spectrum, returning (usi, spectrum, was_skipped)."""
        file_key = (usi.dataset, usi.datafile)

        # Check if file already failed (thread-safe read)
        with failed_files_lock:
            if file_key in failed_files:
                return (usi, None, True)  # Skipped

        spectrum = download_spectrum(usi)

        if spectrum is None:
            # Mark this file as failed (thread-safe write)
            with failed_files_lock:
                if file_key not in failed_files:
                    failed_files.add(file_key)
                    # Log structured failure
                    failure_logger.warning(
                        "Failed to download spectrum",
                        extra={
                            "project_accession": usi.dataset,
                            "ms_run": usi.datafile,
                            "usi": str(usi),
                        },
                    )

        return (usi, spectrum, False)

    # Execute downloads in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_single, usi): usi for usi in usi_list}

        for future in as_completed(futures):
            try:
                usi, spectrum, was_skipped = future.result()
                if was_skipped:
                    skipped += 1
                elif spectrum is None:
                    failures += 1
                else:
                    spectra.append((usi, spectrum))
            except Exception as e:
                # Handle unexpected exceptions from future.result()
                usi = futures[future]
                failures += 1
                failure_logger.error(
                    f"Unexpected error processing future for USI {usi}: {e}",
                    extra={"usi": str(usi)},
                    exc_info=True,
                )

    success_rate = (len(spectra) / len(usi_list)) * 100 if usi_list else 0
    logger.info(
        f"Download complete: {len(spectra)} succeeded, {failures} failed, {skipped} skipped "
        f"({success_rate:.1f}% success rate)"
    )

    return spectra
