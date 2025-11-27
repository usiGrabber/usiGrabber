from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from usigrabber.utils.logging_helpers.aggregator.renderers import Renderer


class FilterOperation(ABC):
    @abstractmethod
    def apply(self, row: dict) -> bool:
        """Return True if row should be included."""
        pass


class ApplyOperation(ABC):
    @abstractmethod
    def apply(self, row: dict) -> dict:
        """Transform the row and return the modified version."""
        pass


class AggregateOperation(ABC):
    @abstractmethod
    def apply(self, existing_row: dict | None, new_row: dict) -> dict:
        """Aggregate new_row into existing_row (or create new if existing is None)."""
        pass

    @abstractmethod
    def get_group_key(self, row: dict) -> str:
        """Return the grouping key for this row."""
        pass


# ==================== Filter Operations ====================


class EventFilter(FilterOperation):
    """Filter rows by event type."""

    def __init__(self, event: str) -> None:
        self._event = event

    def apply(self, row: dict) -> bool:
        return row.get("event") == self._event


class SuccessfulDownloadsFilter(FilterOperation):
    """Filter only successful download events."""

    def apply(self, row: dict) -> bool:
        return row.get("event") == "download_success"


class FailedDownloadsFilter(FilterOperation):
    """Filter only failed download events."""

    def apply(self, row: dict) -> bool:
        return row.get("event") == "download_failure"


class HostFilter(FilterOperation):
    """Filter rows by specific host."""

    def __init__(self, host: str) -> None:
        self._host = host

    def apply(self, row: dict) -> bool:
        return row.get("host") == self._host


class ConnectionEventFilter(FilterOperation):
    """Filter connection open/close events."""

    def apply(self, row: dict) -> bool:
        return row.get("event") in ("connection_open", "connection_close")


class ProjectEventFilter(FilterOperation):
    """Filter project-related events."""

    def apply(self, row: dict) -> bool:
        return row.get("event") in (
            "project_start",
            "project_completed",
            "project_failed",
            "project_skipped",
        )


# ==================== Apply Operations ====================


class ExtractHostFromUrl(ApplyOperation):
    """Extract base host from URL field."""

    def __init__(self, url_field: str = "url") -> None:
        self._url_field = url_field

    def apply(self, row: dict) -> dict:
        from urllib.parse import urlparse

        url = row.get(self._url_field, "")
        if url:
            parsed = urlparse(url)
            row["host"] = parsed.hostname or url
        return row


class AddErrorFlag(ApplyOperation):
    """Add is_error boolean field based on event type."""

    def apply(self, row: dict) -> dict:
        row["is_error"] = row.get("event") in ("download_failure", "download_error")
        return row


# ==================== Aggregate Operations ====================


class Average(AggregateOperation):
    """Calculate running average of a numeric field, grouped by a key."""

    def __init__(self, average_field: str, group_by_field: str = "host") -> None:
        super().__init__()
        self._average_field = average_field
        self._group_by_field = group_by_field

    def get_group_key(self, row: dict) -> str:
        return str(row.get(self._group_by_field, "unknown"))

    def apply(self, existing_row: dict | None, new_row: dict) -> dict:
        if existing_row:
            n = existing_row["_average_n"]
            average = existing_row["_average"]
            new_value = new_row.get(self._average_field, 0)
            new_row["_average"] = (average * n + new_value) / (n + 1)
            new_row["_average_n"] = n + 1
        else:
            new_row["_average"] = new_row.get(self._average_field, 0)
            new_row["_average_n"] = 1
        return new_row


class Counter(AggregateOperation):
    """Count occurrences, grouped by a key."""

    def __init__(self, group_by_field: str = "host") -> None:
        super().__init__()
        self._group_by_field = group_by_field

    def get_group_key(self, row: dict) -> str:
        return str(row.get(self._group_by_field, "unknown"))

    def apply(self, existing_row: dict | None, new_row: dict) -> dict:
        if existing_row:
            new_row["_count"] = existing_row["_count"] + 1
        else:
            new_row["_count"] = 1
        return new_row


class ErrorRate(AggregateOperation):
    """Calculate error rate (requires is_error field), grouped by a key."""

    def __init__(self, group_by_field: str = "host") -> None:
        super().__init__()
        self._group_by_field = group_by_field

    def get_group_key(self, row: dict) -> str:
        return str(row.get(self._group_by_field, "unknown"))

    def apply(self, existing_row: dict | None, new_row: dict) -> dict:
        if existing_row:
            new_row["_total"] = existing_row["_total"] + 1
            new_row["_errors"] = existing_row["_errors"] + (1 if new_row.get("is_error") else 0)
        else:
            new_row["_total"] = 1
            new_row["_errors"] = 1 if new_row.get("is_error") else 0

        new_row["_error_rate"] = (
            (new_row["_errors"] / new_row["_total"] * 100) if new_row["_total"] > 0 else 0
        )
        return new_row


class OpenConnectionsTracker(AggregateOperation):
    """
    Track current open connections per host.
    Increments on connection_open events, decrements on connection_close events.
    """

    def __init__(self, group_by_field: str = "host") -> None:
        super().__init__()
        self._group_by_field = group_by_field

    def get_group_key(self, row: dict) -> str:
        return str(row.get(self._group_by_field, "unknown"))

    def apply(self, existing_row: dict | None, new_row: dict) -> dict:
        event = new_row.get("event")

        current_open = existing_row.get("_open_connections", 0) if existing_row else 0

        # Increment on open, decrement on close
        if event == "connection_open":
            new_row["_open_connections"] = current_open + 1
        elif event == "connection_close":
            new_row["_open_connections"] = max(0, current_open - 1)  # Don't go below 0
        else:
            new_row["_open_connections"] = current_open

        return new_row


class AnalyticsPipeline:
    """
    A pipeline that processes log rows through filter -> apply -> aggregate operations.

    Example:
        # Average response time per host for successful downloads
        pipeline = AnalyticsPipeline(
            name="avg_response_time_per_host",
            filters=[SuccessfulDownloadsFilter()],
            apply_ops=[],
            aggregate_op=Average("response_time", "host"),
            renderer=AverageRenderer()
        )
    """

    def __init__(
        self,
        name: str,
        filters: list[FilterOperation],
        apply_ops: list[ApplyOperation],
        aggregate_op: AggregateOperation,
        renderer: "Renderer | None" = None,
    ) -> None:
        self.name = name
        self.filters = filters
        self.apply_ops = apply_ops
        self.aggregate_op = aggregate_op
        self.renderer = renderer
        self._aggregated_data: dict[str, dict] = {}  # group_key -> aggregated_row

    def process_row(self, row: dict) -> None:
        """Process a single log row through the pipeline."""
        # Step 1: Filter
        for filter_op in self.filters:
            if not filter_op.apply(row):
                return  # Row filtered out

        # Step 2: Apply transformations
        for apply_op in self.apply_ops:
            row = apply_op.apply(row)

        # Step 3: Aggregate
        group_key = self.aggregate_op.get_group_key(row)
        existing = self._aggregated_data.get(group_key)
        self._aggregated_data[group_key] = self.aggregate_op.apply(existing, row)

    def get_results(self) -> dict[str, dict]:
        """Get current aggregated results."""
        return self._aggregated_data.copy()


class RunningLogAggregator:
    """
    Aggregates logs from multiple worker processes using configurable pipelines.

    Example usage:
        aggregator = RunningLogAggregator(log_dir="logs")

        # Register pipelines for different metrics
        aggregator.register_pipeline(AnalyticsPipeline(
            name="avg_response_time",
            filters=[SuccessfulDownloadsFilter()],
            apply_ops=[],
            aggregate_op=Average("response_time", "host")
        ))

        aggregator.register_pipeline(AnalyticsPipeline(
            name="host_error_rates",
            filters=[],
            apply_ops=[AddErrorFlag()],
            aggregate_op=ErrorRate("host")
        ))

        # Update metrics from logs
        aggregator.update()

        # Get results
        metrics = aggregator.get_all_metrics()
    """

    def __init__(self, pids: list[int] | None = None, log_dir: str = "logs") -> None:
        self._pids = pids or []
        self._log_dir = log_dir
        self._file_positions: dict[str, int] = {}  # track position in each log file
        self._pipelines: dict[str, AnalyticsPipeline] = {}

    def register_pipeline(self, pipeline: AnalyticsPipeline) -> None:
        """Register an analytics pipeline."""
        self._pipelines[pipeline.name] = pipeline

    def _read_new_lines(self, file_path: str) -> list[str]:
        """Read only new lines from a log file since last read."""
        import os

        if not os.path.exists(file_path):
            return []

        current_pos = self._file_positions.get(file_path, 0)
        new_lines = []

        try:
            with open(file_path) as f:
                f.seek(current_pos)
                new_lines = f.readlines()
                self._file_positions[file_path] = f.tell()
        except Exception:
            # File might be being written to, skip for now
            pass

        return new_lines

    def _parse_json_line(self, line: str) -> dict | None:
        """Parse a JSON log line."""
        import json

        try:
            return json.loads(line.strip())
        except json.JSONDecodeError:
            return None

    def update(self) -> None:
        """Read new log entries and process through all pipelines."""
        import glob
        import os

        # Find all worker log files
        pattern = os.path.join(self._log_dir, "application-*.jsonl")
        log_files = glob.glob(pattern)

        for log_file in log_files:
            new_lines = self._read_new_lines(log_file)
            for line in new_lines:
                log_entry = self._parse_json_line(line)
                if log_entry:
                    # Process through all registered pipelines
                    for pipeline in self._pipelines.values():
                        pipeline.process_row(log_entry.copy())

    def get_pipeline_results(self, pipeline_name: str) -> dict[str, dict]:
        """Get results from a specific pipeline."""
        pipeline = self._pipelines.get(pipeline_name)
        if pipeline:
            return pipeline.get_results()
        return {}

    def get_all_metrics(self) -> dict[str, dict[str, dict]]:
        """Get results from all pipelines."""
        return {name: pipeline.get_results() for name, pipeline in self._pipelines.items()}
