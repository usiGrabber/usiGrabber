# USI Grabber Web Dashboard

A real-time HTML dashboard for monitoring USI Grabber analytics metrics.

## Features

- **Real-time Updates**: Auto-refreshes every 2 seconds using HTMX
- **Modern UI**: Clean, responsive design with Tailwind CSS
- **Partial Updates**: Only refreshes changed sections for efficiency
- **Low Maintenance**: No build step, no npm dependencies
- **RESTful API**: Fetch individual category data via API endpoints

## Architecture

- **FastAPI**: Lightweight Python web framework
- **HTMX**: Enables partial page updates without complex JavaScript
- **Tailwind CSS**: Utility-first CSS framework (loaded via CDN)
- **Uvicorn**: ASGI server for production-ready performance

## Usage

### Start the Dashboard Server

```bash
usigrabber serve-dashboard
```

The dashboard will be available at `http://127.0.0.1:8000`

### Custom Configuration

```bash
# Custom host and port
usigrabber serve-dashboard --host 0.0.0.0 --port 3000

# Custom log directory
usigrabber serve-dashboard --log-dir /path/to/logs

# Enable auto-reload for development
usigrabber serve-dashboard --reload
```

## API Endpoints

### Full Dashboard
- **GET /** - Returns the complete HTML dashboard

### Category Data
- **GET /api/category/{category_name}** - Returns HTML for a specific category
  - Categories: `FTP_Downloads`, `HTTP_Requests`, `Connections`, `Project_Progress`, `Data_Imported`, `Errors`

### Health Check
- **GET /health** - Returns server health status

## How It Works

1. **Metrics Collection**: The `RunningLogAggregator` reads log files and aggregates metrics
2. **Background Thread**: A daemon thread continuously updates metrics every 2 seconds
3. **Web Server**: FastAPI serves the dashboard and API endpoints
4. **Auto-Refresh**: HTMX polls category endpoints and updates the DOM automatically

## Customization

### Adding New Metrics

1. Define a new `AnalyticsPipeline` in `dashboard.py`
2. Create or reuse a renderer with HTML support
3. Add the pipeline to a category or create a new category
4. The web dashboard will automatically display the new metrics

### Styling

The dashboard uses Tailwind CSS classes. To customize:

- Modify renderer HTML output in `renderers.py`
- Update category layout in `web/app.py`
- All Tailwind utilities are available via CDN

## Performance

- **Efficient Updates**: HTMX only updates changed sections, not the entire page
- **Background Processing**: Metrics aggregation runs in a separate thread
- **Async Server**: Uvicorn handles concurrent requests efficiently
- **Memory Efficient**: Metrics are stored in memory, no database overhead

## Development

To run in development mode with auto-reload:

```bash
usigrabber serve-dashboard --reload
```

Changes to the code will automatically restart the server.
