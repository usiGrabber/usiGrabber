# Renderer Architecture

The monitoring system uses a **decoupled renderer architecture** that separates data processing (pipelines) from data presentation (renderers).

## Architecture Overview

```
AnalyticsPipeline
    ├── Filters (what to process)
    ├── Apply Ops (how to transform)
    ├── Aggregate Op (how to combine)
    └── Renderer (how to display)  ← Pluggable!
```

Each `AnalyticsPipeline` has its own `Renderer` that knows how to format its specific results.

## Why Renderers?

### Before (Tightly Coupled)
```python
# Dashboard had hardcoded display logic for each metric type
if pipeline_name == "open_connections":
    # hardcoded formatting logic...
elif "_average" in data:
    # hardcoded formatting logic...
elif "_error_rate" in data:
    # hardcoded formatting logic...
```

**Problems:**
- Dashboard was tightly coupled to pipeline implementation details
- Adding new metrics required modifying dashboard code
- No way to reuse formatting logic
- Impossible to render to different formats (HTML, JSON, etc.)

### After (Decoupled)
```python
# Each pipeline has its own renderer
pipeline = AnalyticsPipeline(
    name="ftp_avg_response_time",
    filters=[...],
    apply_ops=[...],
    aggregate_op=Average(...),
    renderer=AverageRenderer(unit="s", decimals=2)  # ← Pluggable!
)

# Dashboard just delegates to renderers
for category in categories:
    output = category.render(all_metrics)  # Calls individual renderers
```

**Benefits:**
- ✅ Pipeline and display logic are independent
- ✅ Easy to add new renderers (HTML, JSON, CSV, etc.)
- ✅ Reusable rendering logic
- ✅ Type-safe and testable

## Available Renderers

### Base Renderer
```python
class Renderer(ABC):
    @abstractmethod
    def render(self, results: dict[str, dict]) -> str:
        """Convert pipeline results to a string."""
        pass
```

All renderers implement this interface.

### Built-in Renderers

#### 1. SimpleTextRenderer
Simple key-value display.

```python
renderer = SimpleTextRenderer(value_field="_count")
# Output:
#   host1: 5
#   host2: 3
```

#### 2. AverageRenderer
Displays averages with units and counts.

```python
renderer = AverageRenderer(unit="s", decimals=2, max_items=5)
# Output:
#   ftp.pride.ebi.ac.uk: 2.34s (n=156)
#   www.ebi.ac.uk: 0.45s (n=23)
```

**Parameters:**
- `unit`: Unit suffix (default: "s")
- `decimals`: Decimal places (default: 2)
- `max_items`: Max items to show (default: 5)
- `use_colors`: Enable ANSI colors (default: True)

#### 3. CountRenderer
Displays counts with optional breakdown.

```python
renderer = CountRenderer(show_breakdown=True, max_items=10)
# Output:
#   25 total
#     ConnectionResetError: 15
#     TimeoutError: 10
```

**Parameters:**
- `show_breakdown`: Show individual counts (default: True)
- `max_items`: Max breakdown items (default: 10)

#### 4. ErrorRateRenderer
Displays error rates with color coding.

```python
renderer = ErrorRateRenderer(use_colors=True)
# Output:
#   ftp.pride.ebi.ac.uk: [GREEN]3.2%[RESET] (5/156)
#   old.host.com: [RED]45.0%[RESET] (9/20)
```

**Color Coding:**
- Green: < 5%
- Yellow: 5-15%
- Red: > 15%

**Parameters:**
- `use_colors`: Enable color coding (default: True)
- `max_items`: Max items to show (default: 5)

#### 5. OpenConnectionsRenderer
Displays current open connections.

```python
renderer = OpenConnectionsRenderer(max_items=5)
# Output:
#   8 open connections
#     ftp.pride.ebi.ac.uk: 6
#     www.ebi.ac.uk: 2
```

Only shows hosts with active connections.

### CategoryRenderer

Groups multiple pipelines under a titled category.

```python
category = CategoryRenderer(
    title="📥 FTP Downloads",
    pipelines=[
        ("ftp_avg_response_time", avg_renderer),
        ("ftp_error_rate", error_renderer),
    ],
    width=100
)
```

**Output:**
```
📥 FTP Downloads
────────────────────────────────────
• ftp_avg_response_time:
  ftp.pride.ebi.ac.uk: 2.34s (n=156)
• ftp_error_rate:
  ftp.pride.ebi.ac.uk: 3.2% (5/156)
```

## Creating Custom Renderers

### Example: HTML Renderer

```python
class HTMLRenderer(Renderer):
    """Render metrics as an HTML table."""
    
    def __init__(self, value_field: str = "_count"):
        self.value_field = value_field
    
    def render(self, results: dict[str, dict]) -> str:
        if not results:
            return "<p>No data</p>"
        
        rows = []
        for key, data in results.items():
            value = data.get(self.value_field, 0)
            rows.append(f"<tr><td>{key}</td><td>{value}</td></tr>")
        
        return f"""
        <table>
            <thead>
                <tr><th>Host</th><th>Count</th></tr>
            </thead>
            <tbody>
                {"".join(rows)}
            </tbody>
        </table>
        """
```

**Usage:**
```python
pipeline = AnalyticsPipeline(
    name="downloads_per_host",
    filters=[...],
    apply_ops=[...],
    aggregate_op=Counter("host"),
    renderer=HTMLRenderer()  # ← Custom renderer!
)
```

### Example: JSON Renderer

```python
class JSONRenderer(Renderer):
    """Render metrics as JSON."""
    
    def render(self, results: dict[str, dict]) -> str:
        import json
        return json.dumps(results, indent=2)
```

### Example: Spark Line Renderer

```python
class SparklineRenderer(Renderer):
    """Render a sparkline chart from values."""
    
    def __init__(self, value_field: str = "_average"):
        self.value_field = value_field
        self.chars = "▁▂▃▄▅▆▇█"
    
    def render(self, results: dict[str, dict]) -> str:
        values = [d.get(self.value_field, 0) for d in results.values()]
        if not values:
            return ""
        
        min_val, max_val = min(values), max(values)
        range_val = max_val - min_val or 1
        
        # Map values to sparkline chars
        sparkline = "".join(
            self.chars[int((v - min_val) / range_val * 7)]
            for v in values
        )
        
        return f"{sparkline} (min: {min_val:.2f}, max: {max_val:.2f})"
```

## Dashboard Composition

The dashboard is composed hierarchically:

```
DashboardDisplay
    ├── CategoryRenderer (FTP Downloads)
    │   ├── Pipeline 1 → AverageRenderer
    │   ├── Pipeline 2 → ErrorRateRenderer
    │   └── Pipeline 3 → CountRenderer
    ├── CategoryRenderer (HTTP Requests)
    │   ├── Pipeline 4 → AverageRenderer
    │   └── Pipeline 5 → CountRenderer
    └── CategoryRenderer (Connections)
        └── Pipeline 6 → OpenConnectionsRenderer
```

**Flow:**
1. `DashboardDisplay.display(all_metrics)` is called
2. For each `CategoryRenderer`:
   - `category.render(all_metrics)` is called
   - For each pipeline in the category:
     - `pipeline.renderer.render(results)` is called
     - Results are formatted with bullet points
3. All category outputs are combined with headers/footers

## Future Extensions

### Multiple Output Formats

```python
# Terminal renderer
terminal_display = DashboardDisplay(
    categories=terminal_categories,
    refresh_interval=2.0
)

# HTML renderer for web dashboard
html_display = HTMLDashboard(
    categories=html_categories,
    template="dashboard.html"
)

# Both use the same pipelines, different renderers!
```

### Live Web Dashboard

```python
# Future: HTML renderers for live web updates
html_renderer = HTMLTableRenderer(use_htmx=True)
pipeline = AnalyticsPipeline(
    name="metrics",
    ...,
    renderer=html_renderer
)

# Renders to HTML that auto-refreshes via HTMX
```

### Export to Monitoring Systems

```python
# Export to Prometheus
prometheus_renderer = PrometheusRenderer()

# Export to Grafana JSON
grafana_renderer = GrafanaJSONRenderer()
```

## Summary

The renderer architecture provides:

1. **Separation of Concerns**: Pipelines process data, renderers format it
2. **Extensibility**: Easy to add new output formats
3. **Reusability**: Same renderer works for multiple pipelines
4. **Testability**: Renderers can be tested independently
5. **Type Safety**: Clear interfaces and contracts

This makes the system flexible and future-proof for any display needs!
