"""
PSM batch validation with rate limiting and progress tracking.
"""

import time
from collections import deque
from collections.abc import Sequence
from typing import TYPE_CHECKING

from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn

from usigrabber.db.schema import PeptideSpectrumMatch
from usigrabber.utils import logger
from usigrabber.utils.usi import build_usi
from usigrabber.utils.uuid import UUID

if TYPE_CHECKING:
    from usigrabber.backends.base import BaseBackend


def validate_psms_batch(
    psms: Sequence[PeptideSpectrumMatch],
    backend_cls: type["BaseBackend"],
    requests_per_second: float = 20.0,
    max_concurrent_requests: int = 1,
) -> dict[UUID, bool]:
    """
    Validate a batch of PSMs against a backend repository with rate limiting.

    This function builds USI strings for each PSM, validates them against the
    specified backend (e.g., PRIDE), and returns the validation results. PSMs
    where USI cannot be constructed are automatically marked as invalid (False).

    Uses sliding window rate limiting: maintains a window of recent request timestamps
    to ensure the actual throughput doesn't exceed requests_per_second. Currently
    processes requests sequentially (max_concurrent_requests=1) but designed to
    support concurrent requests in future.

    Args:
        psms: List of PeptideSpectrumMatch objects to validate
        backend_cls: Backend class (e.g., PrideBackend) with validate_usi method
        requests_per_second: Target throughput rate (default: 20.0)
        max_concurrent_requests: Max concurrent requests (default: 1, currently only supports 1)

    Returns:
        Dictionary mapping PSM ID to validation result (True/False)

    Example:
        >>> from usigrabber.backends.pride import PrideBackend
        >>> results = validate_psms_batch(psms, PrideBackend, requests_per_second=10.0)
        >>> results
        {UUID('...'): True, UUID('...'): False, ...}
    """
    if max_concurrent_requests != 1:
        logger.warning(
            f"max_concurrent_requests={max_concurrent_requests} not yet supported, using 1"
        )
        max_concurrent_requests = 1

    results: dict[UUID, bool] = {}
    window_size = 1.0  # 1 second sliding window
    request_timestamps: deque[float] = deque()

    logger.info(
        f"Validating {len(psms)} PSMs with rate limit of {requests_per_second} req/s "
        f"(sliding window: {window_size}s)"
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
    ) as progress:
        task = progress.add_task("Validating PSMs...", total=len(psms))

        for psm in psms:
            # Build USI string
            usi = build_usi(psm)

            if usi is None:
                # Cannot build USI - mark as invalid
                missing_fields = []
                if not psm.project:
                    missing_fields.append("project")
                if not psm.ms_run:
                    missing_fields.append("ms_run")
                if not psm.index_type:
                    missing_fields.append("index_type")
                if psm.index_number is None:
                    missing_fields.append("index_number")
                if not psm.modified_peptide or not psm.modified_peptide.peptide_sequence:
                    missing_fields.append("peptide_sequence")
                if psm.charge_state is None:
                    missing_fields.append("charge_state")

                logger.debug(
                    f"Cannot build USI for PSM {psm.id}: missing {', '.join(missing_fields)}"
                )
                results[psm.id] = False
                progress.advance(task)
                continue

            # Rate limiting: sliding window approach
            current_time = time.time()

            # Remove timestamps outside the window
            while request_timestamps and current_time - request_timestamps[0] > window_size:
                request_timestamps.popleft()

            # Check if we've hit the rate limit
            if len(request_timestamps) >= requests_per_second * window_size:
                # Calculate how long to wait
                oldest_in_window = request_timestamps[0]
                sleep_time = window_size - (current_time - oldest_in_window)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    current_time = time.time()
                    # Clean up old timestamps again after sleeping
                    while request_timestamps and current_time - request_timestamps[0] > window_size:
                        request_timestamps.popleft()

            # Record this request
            request_start = current_time
            request_timestamps.append(request_start)

            # Validate USI against backend
            try:
                is_valid = backend_cls.validate_usi(usi)
                results[psm.id] = is_valid

                request_duration = time.time() - request_start
                if is_valid:
                    logger.debug(f"✓ Valid USI: {usi} ({request_duration:.2f}s)")
                else:
                    logger.debug(f"✗ Invalid USI: {usi} ({request_duration:.2f}s)")

            except Exception as e:
                # Unexpected error - mark as invalid
                request_duration = time.time() - request_start
                logger.error(f"Error validating USI {usi}: {e} ({request_duration:.2f}s)")
                results[psm.id] = False

            progress.advance(task)

    # Summary
    valid_count = sum(1 for v in results.values() if v)
    invalid_count = len(results) - valid_count
    success_rate = (valid_count / len(results) * 100) if results else 0.0
    logger.info(
        f"Validation complete: {valid_count} valid, {invalid_count} invalid "
        f"(success rate: {success_rate:.1f}%)"
    )

    return results
