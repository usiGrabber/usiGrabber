# USI Grabber - Monitoring & Analytics System

## Overview

A comprehensive real-time monitoring system that tracks metrics across your multi-worker data pipeline, including:

- 📥 **FTP Downloads**: Response times, error rates, success counts
- 🌐 **HTTP Requests**: Response times, cache hit rates  
- 🔌 **Connections**: Currently open connections per host
- 📋 **Project Progress**: Completed, failed, and skipped projects
- 📄 **Data Imported**: mzID files parsed, PSMs imported
- ⚠️  **Errors**: Error counts by type and host

## Quick Start

### Run with Live Dashboard

```bash
# Enable dashboard (default when using multiple workers)
usigrabber build --max-workers 4

# Disable dashboard
usigrabber build --max-workers 4 --no-enable-dashboard

# Adjust refresh rate (default: 2 seconds)
usigrabber build --max-workers 4 --dashboard-refresh 5.0
```

The dashboard displays real-time metrics organized by category and automatically updates as workers process data.

## Architecture

### Pipeline-Based Analytics

The system uses a flexible **filter → apply → aggregate** pipeline architecture:

1. **Filter Operations** - Select which log entries to process
2. **Apply Operations** - Transform log entries (map phase)
3. **Aggregate Operations** - Combine entries into metrics (reduce phase)

### Structured Logging

All components emit structured JSON logs with `extra` fields:

**FTP Downloads:**
```python
logger.info("Downloaded file", extra={
    "event": "download_success",
    "url": "ftp://...",
    "host": "ftp.pride.ebi.ac.uk",
    "response_time": 2.34,
    "attempts": 1
})
```

**HTTP Requests:**
```python
logger.info("HTTP request", extra={
    "event": "http_success",
    "host": "www.ebi.ac.uk",
    "status_code": 200,
    "response_time": 0.45,
    "is_cached": False
})
```

**Connection Tracking:**
```python
# Opening connection
logger.debug("Opening connection", extra={
    "event": "connection_open",
    "protocol": "ftp",
    "host": "ftp.pride.ebi.ac.uk"
})

# Closing connection
logger.debug("Closing connection", extra={
    "event": "connection_close",
    "protocol": "ftp",
    "host": "ftp.pride.ebi.ac.uk"
})
```

**Project Progress:**
```python
logger.info("Project completed", extra={
    "event": "project_completed",
    "project_accession": "PXD000001",
    "backend": "PRIDE"
})

logger.info("mzID imported", extra={
    "event": "mzid_imported",
    "project_accession": "PXD000001",
    "psm_count": 12543,
    "parse_time": 1.23
})
```

## Tracked Metrics

### Download Metrics
- **FTP Response Time**: Average time per host for successful downloads
- **FTP Error Rate**: Percentage of failed downloads per host
- **FTP Download Count**: Total successful downloads per host

### HTTP Metrics
- **HTTP Response Time**: Average request time per host
- **HTTP Cache Hits**: Number of cache hits per host (excludes cached requests from response time calculations)

### Connection Metrics
- **Open Connections**: Real-time count of open connections per host
  - Increments on `connection_open` events
  - Decrements on `connection_close` events
  - Tracks both HTTP and FTP connections

### Progress Metrics
- **Projects Completed**: Count per backend
- **Projects Failed**: Count per backend  
- **Projects Skipped**: Count with reason
- **mzID Files Imported**: Total count
- **PSMs Imported**: Total count across all files

### Error Metrics
- **Errors by Type**: Count of each error type (`ConnectionResetError`, `TimeoutError`, etc.)
- **Error Rate by Host**: Percentage of requests that failed per host

## Default Dashboard Pipelines

### 1. FTP Average Response Time
```python
AnalyticsPipeline(
    name="ftp_avg_response_time",
    filters=[SuccessfulDownloadsFilter()],
    apply_ops=[],
    aggregate_op=Average("response_time", "host")
)
```
**Shows**: Average download time per FTP host (successful downloads only)

### 2. FTP Error Rate
```python
AnalyticsPipeline(
    name="ftp_error_rate",
    filters=[],
    apply_ops=[AddErrorFlag()],
    aggregate_op=ErrorRate("host")
)
```
**Shows**: Error percentage per host (color-coded: green <5%, yellow <15%, red ≥15%)

### 3. Open Connections
```python
AnalyticsPipeline(
    name="open_connections",
    filters=[ConnectionEventFilter()],
    apply_ops=[],
    aggregate_op=OpenConnectionsTracker("host")
)
```
**Shows**: Current number of open connections per host in real-time

### 4. HTTP Cache Hits
```python
AnalyticsPipeline(
    name="http_cache_hits",
    filters=[EventFilter("http_cache_hit")],
    apply_ops=[],
    aggregate_op=Counter("host")
)
```
**Shows**: Total cache hits per host (cached requests don't affect response time metrics)

### 5. Projects Completed
```python
AnalyticsPipeline(
    name="projects_completed",
    filters=[EventFilter("project_completed")],
    apply_ops=[],
    aggregate_op=Counter("backend")
)
```
**Shows**: Total projects successfully processed per backend

### 6. mzID Files Imported
```python
AnalyticsPipeline(
    name="mzid_files_imported",
    filters=[EventFilter("mzid_imported")],
    apply_ops=[],
    aggregate_op=Counter("project_accession")
)
```
**Shows**: Total mzID files parsed and imported

## Custom Analytics

### Example: Slow Request Detector

```python
from usigrabber.utils.logging_helpers.aggregator import (
    AnalyticsPipeline,
    FilterOperation,
    Counter,
    RunningLogAggregator
)

class SlowRequestFilter(FilterOperation):
    def apply(self, row: dict) -> bool:
        return row.get("response_time", 0) > 10.0

# Create custom pipeline
aggregator = RunningLogAggregator(log_dir="logs")
aggregator.register_pipeline(AnalyticsPipeline(
    name="slow_requests",
    filters=[SlowRequestFilter()],
    apply_ops=[],
    aggregate_op=Counter("host")
))

aggregator.update()
results = aggregator.get_pipeline_results("slow_requests")
```

### Example: Cache Hit Rate Calculator

```python
class CacheHitRate(AggregateOperation):
    def __init__(self):
        self._group_by_field = "host"
    
    def get_group_key(self, row: dict) -> str:
        return str(row.get(self._group_by_field, "unknown"))
    
    def apply(self, existing: dict | None, new: dict) -> dict:
        if existing:
            total = existing["_total"] + 1
            hits = existing["_hits"] + (1 if new.get("event") == "http_cache_hit" else 0)
        else:
            total = 1
            hits = 1 if new.get("event") == "http_cache_hit" else 0
        
        new["_total"] = total
        new["_hits"] = hits
        new["_hit_rate"] = (hits / total * 100) if total > 0 else 0
        return new
```

## Log File Structure

Worker processes write JSON logs to:
```
logs/application-{PID}.jsonl    # JSON structured logs
logs/application-{PID}.log      # Human-readable logs
```

Main process writes to:
```
logs/application-main.jsonl
logs/application-main.log
```

## Implementation Details

### Connection Tracking

Connections are tracked with increment/decrement counters:

**FTP:**
- `connection_open`: Logged when `aioftp.Client.context` enters
- `connection_close`: Logged when context exits (success or error)

**HTTP:**
- `connection_open`: Logged before `session.get()`
- `connection_close`: Logged after response (success or error)

The `OpenConnectionsTracker` aggregate maintains a running count:
```python
if event == "connection_open":
    count += 1
elif event == "connection_close":
    count = max(0, count - 1)
```

### Cache Hit Handling

HTTP cache hits are logged separately with `http_cache_hit` event and excluded from normal response time calculations. This prevents skewed metrics where cached responses (microseconds) mix with network requests (seconds).

## Available Operations

### Filters
- `EventFilter(event)` - Filter by event type
- `SuccessfulDownloadsFilter()` - Only successful downloads
- `FailedDownloadsFilter()` - Only failed downloads
- `HostFilter(host)` - Filter by specific host
- `ConnectionEventFilter()` - Only connection open/close events
- `ProjectEventFilter()` - Only project-related events

### Apply Operations
- `ExtractHostFromUrl(field)` - Extract hostname from URL
- `AddErrorFlag()` - Add `is_error` boolean field

### Aggregate Operations
- `Average(field, group_by)` - Calculate running average
- `Counter(group_by)` - Count occurrences
- `ErrorRate(group_by)` - Calculate error percentage
- `OpenConnectionsTracker(group_by)` - Track open connections

## See Also

- [`src/usigrabber/utils/logging_helpers/aggregator/README.md`](src/usigrabber/utils/logging_helpers/aggregator/README.md) - Detailed pipeline documentation
- [`examples/custom_analytics_pipeline.py`](examples/custom_analytics_pipeline.py) - Custom pipeline examples
- [`src/usigrabber/utils/logging_helpers/aggregator/running_aggregator.py`](src/usigrabber/utils/logging_helpers/aggregator/running_aggregator.py) - Core implementation
- [`src/usigrabber/utils/logging_helpers/aggregator/dashboard.py`](src/usigrabber/utils/logging_helpers/aggregator/dashboard.py) - Dashboard display
