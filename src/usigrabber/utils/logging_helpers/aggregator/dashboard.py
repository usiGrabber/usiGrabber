"""
Live dashboard for displaying aggregated metrics to console.
"""

import sys

from usigrabber.utils.logging_helpers.aggregator.filters import (
    ConnectionEventFilter,
    DownloadEventFilter,
    EventFilter,
    LevelFilter,
    SuccessfulDownloadsFilter,
)
from usigrabber.utils.logging_helpers.aggregator.renderers import CategoryRenderer


class DashboardDisplay:
    """
    Displays aggregated metrics in a live console dashboard.

    Uses renderers to format output and ANSI escape codes to update the display in place.
    """

    def __init__(
        self,
        categories: list[CategoryRenderer],
        refresh_interval: float = 2.0,
        width: int = 100,
    ) -> None:
        """
        Args:
            categories: List of CategoryRenderer instances to display
            refresh_interval: How often to refresh the display in seconds
            width: Width of the dashboard
        """
        self.categories = categories
        self.refresh_interval = refresh_interval
        self.width = width
        self._last_line_count = 0

    def _clear_previous_output(self) -> None:
        """Clear the previous dashboard output from terminal."""
        if self._last_line_count > 0:
            # Move cursor up and clear lines
            for _ in range(self._last_line_count):
                sys.stdout.write("\033[F")  # Move cursor up one line
                sys.stdout.write("\033[K")  # Clear line
            sys.stdout.flush()

    def display(self, all_metrics: dict[str, dict[str, dict]]) -> None:
        """
        Display the metrics dashboard using configured renderers.

        Args:
            all_metrics: Dict mapping pipeline names to their results
        """
        self._clear_previous_output()

        lines = []
        lines.append("=" * self.width)
        lines.append("📊 USI Grabber - Live Metrics Dashboard".center(self.width))
        lines.append("=" * self.width)
        lines.append("")

        # Render each category
        for category in self.categories:
            category_output = category.render(all_metrics)
            lines.append(category_output)
            lines.append("")

        lines.append("=" * self.width)
        lines.append("Press Ctrl+C to stop monitoring".center(self.width))
        lines.append("=" * self.width)

        output = "\n".join(lines)
        self._last_line_count = len(lines)

        print(output, flush=True)


def create_default_dashboard() -> tuple[list, list[CategoryRenderer]]:
    """
    Create default pipelines and dashboard categories with renderers.

    Returns:
        Tuple of (pipelines, categories) for the dashboard
    """
    from usigrabber.utils.logging_helpers.aggregator.renderers import (
        AverageRenderer,
        CategoryRenderer,
        CountRenderer,
        DatabasePoolRenderer,
        ErrorRateRenderer,
        OpenConnectionsRenderer,
        SessionMemoryRenderer,
    )
    from usigrabber.utils.logging_helpers.aggregator.running_aggregator import (
        AddErrorFlag,
        AnalyticsPipeline,
        Average,
        Counter,
        ErrorRate,
        LatestValue,
        OpenConnectionsTracker,
    )

    # ========== Create Pipelines with Renderers ==========

    ftp_avg_response = AnalyticsPipeline(
        name="ftp_avg_response_time",
        filters=[SuccessfulDownloadsFilter()],
        apply_ops=[],
        aggregate_op=Average("response_time", "host"),
        renderer=AverageRenderer(unit="s", decimals=2),
    )

    ftp_error_rate = AnalyticsPipeline(
        name="ftp_error_rate",
        filters=[DownloadEventFilter()],
        apply_ops=[AddErrorFlag()],
        aggregate_op=ErrorRate("host"),
        renderer=ErrorRateRenderer(),
    )

    ftp_downloads_count = AnalyticsPipeline(
        name="ftp_downloads_count",
        filters=[SuccessfulDownloadsFilter()],
        apply_ops=[],
        aggregate_op=Counter("host"),
        renderer=CountRenderer(show_breakdown=False),
    )

    http_cache_hits = AnalyticsPipeline(
        name="http_cache_hits",
        filters=[EventFilter("http_cache_hit")],
        apply_ops=[],
        aggregate_op=Counter("host"),
        renderer=CountRenderer(show_breakdown=False),
    )

    http_avg_response = AnalyticsPipeline(
        name="http_avg_response_time",
        filters=[EventFilter("http_success")],
        apply_ops=[],
        aggregate_op=Average("response_time", "host"),
        renderer=AverageRenderer(unit="s", decimals=2),
    )

    open_connections = AnalyticsPipeline(
        name="open_connections",
        filters=[ConnectionEventFilter()],
        apply_ops=[],
        aggregate_op=OpenConnectionsTracker("host"),
        renderer=OpenConnectionsRenderer(),
    )

    projects_completed = AnalyticsPipeline(
        name="projects_completed",
        filters=[EventFilter("project_completed")],
        apply_ops=[],
        aggregate_op=Counter("backend"),
        renderer=CountRenderer(show_breakdown=True),
    )

    projects_skipped = AnalyticsPipeline(
        name="project_skipped",
        filters=[EventFilter("project_skipped")],
        apply_ops=[],
        aggregate_op=Counter("backend"),
        renderer=CountRenderer(show_breakdown=True),
    )

    projects_failed = AnalyticsPipeline(
        name="projects_failed",
        filters=[EventFilter("project_failed")],
        apply_ops=[],
        aggregate_op=Counter("backend"),
        renderer=CountRenderer(show_breakdown=True),
    )

    mzid_files_imported = AnalyticsPipeline(
        name="mzid_files_imported",
        filters=[EventFilter("mzid_imported")],
        apply_ops=[],
        aggregate_op=Counter("project_accession"),
        renderer=CountRenderer(show_breakdown=False),
    )

    errors_by_type = AnalyticsPipeline(
        name="errors_by_type",
        filters=[EventFilter("download_failure")],
        apply_ops=[],
        aggregate_op=Counter("error_type"),
        renderer=CountRenderer(show_breakdown=True),
    )

    warnings = AnalyticsPipeline(
        name="warnings by module",
        filters=[LevelFilter("warning")],
        apply_ops=[],
        aggregate_op=Counter("name"),
        renderer=CountRenderer(show_breakdown=True),
    )

    session_memory_before_commit = AnalyticsPipeline(
        name="session_memory_before_commit",
        filters=[EventFilter("session_memory_before_commit")],
        apply_ops=[],
        aggregate_op=LatestValue("project_accession", ["identity_map_size"]),
        renderer=SessionMemoryRenderer(),
    )

    session_memory_after_expunge = AnalyticsPipeline(
        name="session_memory_after_expunge",
        filters=[EventFilter("session_memory_after_expire")],
        apply_ops=[],
        aggregate_op=LatestValue("project_accession", ["identity_map_size"]),
        renderer=SessionMemoryRenderer(),
    )

    db_pool_status_start = AnalyticsPipeline(
        name="db_pool_status_start",
        filters=[EventFilter("db_pool_status_start")],
        apply_ops=[],
        aggregate_op=LatestValue("event", ["pool_size", "checked_in", "checked_out", "overflow"]),
        renderer=DatabasePoolRenderer(),
    )

    db_pool_status_end = AnalyticsPipeline(
        name="db_pool_status_end",
        filters=[EventFilter("db_pool_status_end")],
        apply_ops=[],
        aggregate_op=LatestValue("event", ["pool_size", "checked_in", "checked_out", "overflow"]),
        renderer=DatabasePoolRenderer(),
    )

    pipelines = [
        ftp_avg_response,
        ftp_error_rate,
        ftp_downloads_count,
        http_cache_hits,
        http_avg_response,
        open_connections,
        projects_skipped,
        projects_completed,
        projects_failed,
        mzid_files_imported,
        errors_by_type,
        warnings,
        session_memory_before_commit,
        session_memory_after_expunge,
        db_pool_status_start,
        db_pool_status_end,
    ]

    # ========== Create Categories ==========

    categories = [
        CategoryRenderer(
            title="📋 Project Progress",
            pipelines=[
                ("projects_completed", projects_completed.renderer),
                ("project_skipped", projects_skipped.renderer),
                ("projects_failed", projects_failed.renderer),
            ],
        ),
        CategoryRenderer(
            title="💾 Memory & Database",
            pipelines=[
                ("session_memory_before_commit", session_memory_before_commit.renderer),
                ("session_memory_after_expunge", session_memory_after_expunge.renderer),
                ("db_pool_status_start", db_pool_status_start.renderer),
                ("db_pool_status_end", db_pool_status_end.renderer),
            ],
        ),
        CategoryRenderer(
            title="📥 FTP Downloads",
            pipelines=[
                ("ftp_avg_response_time", ftp_avg_response.renderer),
                ("ftp_error_rate", ftp_error_rate.renderer),
                ("ftp_downloads_count", ftp_downloads_count.renderer),
            ],
        ),
        CategoryRenderer(
            title="🌐 HTTP Requests",
            pipelines=[
                ("http_avg_response_time", http_avg_response.renderer),
                ("http_cache_hits", http_cache_hits.renderer),
            ],
        ),
        CategoryRenderer(
            title="🔌 Connections",
            pipelines=[
                ("open_connections", open_connections.renderer),
            ],
        ),
        CategoryRenderer(
            title="📄 Data Imported",
            pipelines=[
                ("mzid_files_imported", mzid_files_imported.renderer),
            ],
        ),
        CategoryRenderer(
            title="⚠️  Errors and Warnings",
            pipelines=[
                ("errors_by_type", errors_by_type.renderer),
                ("warnings by module", warnings.renderer),
            ],
        ),
    ]

    return pipelines, categories
