# Log Aggregator & Analytics Pipeline

A flexible, composable analytics system for aggregating and analyzing JSON logs from multiple worker processes in real-time.

## Overview

The system uses a **pipeline-based architecture** with three types of operations:

1. **Filter Operations** - Decide which log entries to process
2. **Apply Operations** - Transform log entries (map)
3. **Aggregate Operations** - Combine entries into metrics (reduce)

Each analytics query is a pipeline: `Filter → Apply → Aggregate`

## Quick Start

### Using the Built-in Dashboard

The dashboard is automatically enabled when running the build command with multiple workers:

```bash
# Dashboard enabled by default
usigrabber build --max-workers 4

# Disable dashboard
usigrabber build --max-workers 4 --no-enable-dashboard

# Adjust refresh rate
usigrabber build --max-workers 4 --dashboard-refresh 5.0
```

### Creating Custom Analytics Pipelines

```python
from usigrabber.utils.logging_helpers.aggregator import (
    AnalyticsPipeline,
    RunningLogAggregator,
    SuccessfulDownloadsFilter,
    Average,
)

# Create aggregator
aggregator = RunningLogAggregator(log_dir="logs")

# Define a pipeline: "What's the average response time per host for successful downloads?"
pipeline = AnalyticsPipeline(
    name="avg_response_time_per_host",
    filters=[SuccessfulDownloadsFilter()],  # Only successful downloads
    apply_ops=[],  # No transformations needed
    aggregate_op=Average("response_time", "host")  # Average by host
)

# Register and run
aggregator.register_pipeline(pipeline)
aggregator.update()  # Read new log entries

# Get results
results = aggregator.get_pipeline_results("avg_response_time_per_host")
for host, data in results.items():
    print(f"{host}: {data['_average']:.2f}s (n={data['_average_n']})")
```

## Available Operations

### Filter Operations

| Operation | Description | Example |
|-----------|-------------|---------|
| `EventFilter(event)` | Filter by event type | `EventFilter("download_success")` |
| `SuccessfulDownloadsFilter()` | Only successful downloads | - |
| `FailedDownloadsFilter()` | Only failed downloads | - |
| `HostFilter(host)` | Filter by specific host | `HostFilter("ftp.pride.ebi.ac.uk")` |

### Apply Operations

| Operation | Description | Example |
|-----------|-------------|---------|
| `ExtractHostFromUrl(field)` | Extract hostname from URL | `ExtractHostFromUrl("url")` |
| `AddErrorFlag()` | Add `is_error` boolean field | - |

### Aggregate Operations

All aggregate operations group by a field (default: `"host"`).

| Operation | Description | Output Fields | Example |
|-----------|-------------|---------------|---------|
| `Average(field, group_by)` | Running average | `_average`, `_average_n` | `Average("response_time", "host")` |
| `Counter(group_by)` | Count occurrences | `_count` | `Counter("error_type")` |
| `ErrorRate(group_by)` | Calculate error rate | `_error_rate`, `_total`, `_errors` | `ErrorRate("host")` |

## Example Analytics Queries

### 1. Average Response Time per Host

```python
AnalyticsPipeline(
    name="avg_response_time",
    filters=[SuccessfulDownloadsFilter()],
    apply_ops=[],
    aggregate_op=Average("response_time", "host")
)
```

**Question**: What's the average response time for successful downloads from each host?

### 2. Error Rate per Host

```python
AnalyticsPipeline(
    name="error_rate",
    filters=[],  # All events
    apply_ops=[AddErrorFlag()],
    aggregate_op=ErrorRate("host")
)
```

**Question**: What percentage of requests fail for each host?

### 3. Count Errors by Type

```python
AnalyticsPipeline(
    name="errors_by_type",
    filters=[EventFilter("download_failure")],
    apply_ops=[],
    aggregate_op=Counter("error_type")
)
```

**Question**: How many of each error type occurred?

### 4. Downloads per Host

```python
AnalyticsPipeline(
    name="downloads_per_host",
    filters=[SuccessfulDownloadsFilter()],
    apply_ops=[],
    aggregate_op=Counter("host")
)
```

**Question**: How many successful downloads from each host?

## Implementing Custom Operations

### Custom Filter

```python
from usigrabber.utils.logging_helpers.aggregator import FilterOperation

class SlowRequestFilter(FilterOperation):
    """Filter requests slower than a threshold."""
    
    def __init__(self, threshold_seconds: float):
        self.threshold = threshold_seconds
    
    def apply(self, row: dict) -> bool:
        response_time = row.get("response_time", 0)
        return response_time > self.threshold
```

### Custom Apply Operation

```python
from usigrabber.utils.logging_helpers.aggregator import ApplyOperation

class CategorizeResponseTime(ApplyOperation):
    """Categorize response time as fast/medium/slow."""
    
    def apply(self, row: dict) -> dict:
        rt = row.get("response_time", 0)
        if rt < 1:
            row["speed_category"] = "fast"
        elif rt < 5:
            row["speed_category"] = "medium"
        else:
            row["speed_category"] = "slow"
        return row
```

### Custom Aggregate Operation

```python
from usigrabber.utils.logging_helpers.aggregator import AggregateOperation

class Max(AggregateOperation):
    """Track maximum value of a field."""
    
    def __init__(self, field: str, group_by: str = "host"):
        self.field = field
        self.group_by = group_by
    
    def get_group_key(self, row: dict) -> str:
        return str(row.get(self.group_by, "unknown"))
    
    def apply(self, existing: dict | None, new: dict) -> dict:
        if existing:
            new["_max"] = max(existing["_max"], new.get(self.field, 0))
        else:
            new["_max"] = new.get(self.field, 0)
        return new
```

## Log Entry Format

The aggregator expects JSON log entries with these fields:

```json
{
  "event": "download_success",
  "url": "ftp://ftp.pride.ebi.ac.uk/path/to/file.mzid",
  "host": "ftp.pride.ebi.ac.uk",
  "response_time": 2.34,
  "attempts": 1
}
```

For failures:

```json
{
  "event": "download_failure",
  "error_type": "ConnectionResetError",
  "url": "ftp://ftp.pride.ebi.ac.uk/path/to/file.mzid",
  "host": "ftp.pride.ebi.ac.uk",
  "response_time": 15.0,
  "attempts": 3
}
```

## Architecture

```
Log Files (per worker)
    ↓
RunningLogAggregator
    ↓
[Pipeline 1] [Pipeline 2] [Pipeline 3] ...
    ↓
Filter → Apply → Aggregate
    ↓
Results (grouped by key)
    ↓
DashboardDisplay
```

Each pipeline independently processes all log entries and maintains its own aggregated state.

## See Also

- `examples/custom_analytics_pipeline.py` - Complete working example
- `dashboard.py` - Dashboard display implementation
- `running_aggregator.py` - Core aggregator and operations
