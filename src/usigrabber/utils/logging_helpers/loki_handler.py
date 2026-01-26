"""Custom Loki logging handler with proper error handling."""

import logging
import sys
import threading
import time
from collections import deque
from typing import Any

import requests

from usigrabber.utils.context import context_file_id, context_project_accession


class LokiHandler(logging.Handler):
    """
    Custom Loki logging handler that sends logs to Grafana Loki.

    Batches logs and sends them asynchronously to avoid blocking the application.
    Handles errors gracefully by logging to stderr instead of silently failing.
    """

    def __init__(
        self,
        url: str,
        tags: dict[str, str] | None = None,
        batch_size: int = 100,
        flush_interval: float = 5.0,
        timeout: float = 10.0,
        include_metadata: bool = True,
        use_structured_metadata: bool = False,
    ):
        """
        Initialize the Loki handler.

        Args:
            url: Loki push API URL (e.g., http://localhost:3100/loki/api/v1/push)
            tags: Dictionary of labels to attach to all logs
            batch_size: Maximum number of logs to batch before sending
            flush_interval: Maximum time in seconds to wait before flushing logs
            timeout: HTTP request timeout in seconds
            include_metadata: Whether to include log record metadata (level, file, line, etc.)
            use_structured_metadata: If True, send as Loki structured metadata (requires Loki config).
                                      If False, append metadata as JSON to log message.
        """
        super().__init__()
        self.url = url
        self.tags = tags or {}
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.timeout = timeout
        self.include_metadata = include_metadata
        self.use_structured_metadata = use_structured_metadata

        self._buffer: deque[tuple[int, str, dict[str, str]]] = deque()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._flush_thread = threading.Thread(target=self._flush_worker, daemon=True)
        self._flush_thread.start()

        self._session = requests.Session()
        self._last_error_time = 0.0
        self._error_throttle = 60.0  # Only log errors once per minute to avoid spam

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record by adding it to the buffer.

        Args:
            record: The log record to emit
        """
        try:
            msg = self.format(record)
            timestamp_ns = int(record.created * 1e9)  # Convert to nanoseconds

            structured_metadata = {}
            if self.include_metadata:
                # Extract structured metadata from the record
                structured_metadata = {
                    "level": record.levelname,
                    "logger": record.name,
                    "file": record.filename,
                    "line": str(record.lineno),
                    "function": record.funcName,
                    "process": str(record.process),
                    "thread": str(record.thread),
                }

                # Add any extra fields from the record
                if hasattr(record, "__dict__"):
                    for key, value in record.__dict__.items():
                        if key not in [
                            "name",
                            "msg",
                            "args",
                            "created",
                            "filename",
                            "funcName",
                            "levelname",
                            "levelno",
                            "lineno",
                            "module",
                            "msecs",
                            "pathname",
                            "process",
                            "processName",
                            "relativeCreated",
                            "thread",
                            "threadName",
                            "exc_info",
                            "exc_text",
                            "stack_info",
                            "taskName",
                            "getMessage",
                        ]:
                            structured_metadata[key] = str(value)

            # Add context variables if set
            project_accession = context_project_accession.get(None)
            if project_accession:
                structured_metadata["project_accession"] = project_accession

            file_id = context_file_id.get(None)
            if file_id:
                structured_metadata["file_id"] = file_id

            with self._lock:
                self._buffer.append((timestamp_ns, msg, structured_metadata))

                # Flush if buffer is full
                if len(self._buffer) >= self.batch_size:
                    self._flush_buffer()

        except Exception as e:
            self._log_error(f"Error in LokiHandler.emit: {e}")

    def _flush_buffer(self) -> None:
        if not self._buffer:
            return

        values = []

        for ts, msg, metadata in self._buffer:
            # Always treat the message as structured logfmt
            logfmt_fields = {}

            # Message first (this is the key fix)
            logfmt_fields["msg"] = msg

            # Add metadata fields
            if metadata:
                logfmt_fields.update(metadata)

            # Build logfmt string
            parts = []
            for key, value in logfmt_fields.items():
                escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
                parts.append(f'{key}="{escaped}"')

            log_line = " ".join(parts)
            values.append([str(ts), log_line])

        payload = {
            "streams": [
                {
                    "stream": self.tags,
                    "values": values,
                }
            ]
        }

        self._buffer.clear()

        threading.Thread(
            target=self._send_to_loki,
            args=(payload, len(values)),
            daemon=True,
        ).start()

    def _flush_worker(self) -> None:
        """Background worker that periodically flushes the buffer."""
        while not self._stop_event.is_set():
            time.sleep(self.flush_interval)
            with self._lock:
                if self._buffer:
                    self._flush_buffer()

    def _send_to_loki(self, payload: dict[str, Any], batch_size: int) -> None:
        """
        Send payload to Loki API.

        Args:
            payload: The Loki payload to send
            batch_size: Number of logs in this batch (for error reporting)
        """
        try:
            response = self._session.post(
                self.url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )

            if response.status_code not in (200, 204):
                self._log_error(
                    f"Loki API returned status {response.status_code}: {response.text[:200]}"
                )

        except requests.exceptions.Timeout:
            self._log_error(f"Timeout sending {batch_size} logs to Loki (>{self.timeout}s)")
        except requests.exceptions.ConnectionError as e:
            self._log_error(f"Connection error sending logs to Loki: {e}")
        except Exception as e:
            self._log_error(f"Unexpected error sending logs to Loki: {e}")

    def _log_error(self, message: str) -> None:
        """
        Log errors to stderr with throttling to avoid spam.

        Args:
            message: Error message to log
        """
        current_time = time.time()
        if current_time - self._last_error_time >= self._error_throttle:
            print(f"[LokiHandler ERROR] {message}", file=sys.stderr)
            self._last_error_time = current_time

    def flush(self) -> None:
        """Flush any buffered logs."""
        with self._lock:
            self._flush_buffer()

    def close(self) -> None:
        """Close the handler and clean up resources."""
        self._stop_event.set()
        self.flush()
        self._flush_thread.join(timeout=5.0)
        self._session.close()
        super().close()
