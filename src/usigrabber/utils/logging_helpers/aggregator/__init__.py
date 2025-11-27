"""
Log aggregator and analytics pipeline for monitoring worker processes.
"""

from usigrabber.utils.logging_helpers.aggregator.dashboard import (
    DashboardDisplay,
    create_default_dashboard,
)
from usigrabber.utils.logging_helpers.aggregator.renderers import (
    AverageRenderer,
    CategoryRenderer,
    CountRenderer,
    ErrorRateRenderer,
    OpenConnectionsRenderer,
    Renderer,
    SimpleTextRenderer,
)
from usigrabber.utils.logging_helpers.aggregator.running_aggregator import (
    AddErrorFlag,
    AnalyticsPipeline,
    Average,
    ConnectionEventFilter,
    Counter,
    ErrorRate,
    EventFilter,
    ExtractHostFromUrl,
    FailedDownloadsFilter,
    HostFilter,
    OpenConnectionsTracker,
    ProjectEventFilter,
    RunningLogAggregator,
    SuccessfulDownloadsFilter,
)

__all__ = [
    # Core classes
    "RunningLogAggregator",
    "AnalyticsPipeline",
    "DashboardDisplay",
    # Filter operations
    "EventFilter",
    "SuccessfulDownloadsFilter",
    "FailedDownloadsFilter",
    "HostFilter",
    "ConnectionEventFilter",
    "ProjectEventFilter",
    # Apply operations
    "ExtractHostFromUrl",
    "AddErrorFlag",
    # Aggregate operations
    "Average",
    "Counter",
    "ErrorRate",
    "OpenConnectionsTracker",
    # Renderers
    "Renderer",
    "SimpleTextRenderer",
    "AverageRenderer",
    "CountRenderer",
    "ErrorRateRenderer",
    "OpenConnectionsRenderer",
    "CategoryRenderer",
    # Utilities
    "create_default_dashboard",
]
