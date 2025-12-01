from typing import Literal

from usigrabber.utils.logging_helpers.aggregator.running_aggregator import FilterOperation


class EventFilter(FilterOperation):
    """Filter rows by event type."""

    def __init__(self, event: str) -> None:
        self._event = event

    def apply(self, row: dict) -> bool:
        return row.get("event") == self._event


LOG_LEVEL = Literal["error", "warning", "info", "debug", "critial"]


class LevelFilter(FilterOperation):
    def __init__(self, level: LOG_LEVEL) -> None:
        super().__init__()
        self._level = level

    def apply(self, row: dict) -> bool:
        return row.get("level", "").lower() == self._level.lower()


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


class DownloadEventFilter(FilterOperation):
    """Filter download-related events (success, failure, error)."""

    def apply(self, row: dict) -> bool:
        return row.get("event") in (
            "download_success",
            "download_failure",
            "download_error",
        )
