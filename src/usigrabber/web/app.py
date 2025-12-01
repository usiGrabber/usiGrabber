"""FastAPI application for the USI Grabber analytics dashboard."""

from typing import Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from usigrabber.utils.logging_helpers.aggregator.dashboard import create_default_dashboard

app = FastAPI(title="USI Grabber Dashboard", version="1.0.0")


# Global state to store metrics
# In production, this would be populated by the analytics pipeline
_metrics_store: dict[str, dict[str, dict]] = {}
_stats_store: dict[str, dict[str, int | list[str]]] = {}
_log_files_store: list[dict[str, str | int]] = []


def update_metrics(metrics: dict[str, dict[str, dict]]) -> None:
    """Update the metrics store with new data from the analytics pipeline."""
    global _metrics_store
    _metrics_store = metrics


def get_metrics() -> dict[str, dict[str, dict]]:
    """Get the current metrics."""
    return _metrics_store


def update_stats(stats: dict[str, dict[str, int | list[str]]]) -> None:
    """Update the stats store with new data from the analytics pipeline."""
    global _stats_store
    _stats_store = stats


def get_stats() -> dict[str, dict[str, int | list[str]]]:
    """Get the current stats."""
    return _stats_store


def update_log_files(log_files: list[dict[str, str | int]]) -> None:
    """Update the log files store with new data."""
    global _log_files_store
    _log_files_store = log_files


def get_log_files() -> list[dict[str, str | int]]:
    """Get the current log files."""
    return _log_files_store


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    """Render the full HTML dashboard."""
    _, categories = create_default_dashboard()
    metrics = get_metrics()

    html_parts = [
        "<!DOCTYPE html>",
        '<html lang="en">',
        "<head>",
        '    <meta charset="UTF-8">',
        '    <meta name="viewport" content="width=device-width, initial-scale=1.0">',
        "    <title>USI Grabber - Analytics Dashboard</title>",
        '    <script src="https://cdn.tailwindcss.com"></script>',
        '    <script src="https://unpkg.com/htmx.org@2.0.4"></script>',
        "</head>",
        '<body class="bg-gray-50">',
        '    <div class="min-h-screen">',
        "        <!-- Header -->",
        '        <header class="bg-gradient-to-r from-indigo-600 to-purple-600 text-white shadow-lg">',
        '            <div class="max-w-7xl mx-auto px-4 py-6">',
        '                <h1 class="text-3xl font-bold">USI Grabber Analytics Dashboard</h1>',
        '                <p class="text-indigo-100 mt-1">Real-time metrics and performance monitoring</p>',
        "            </div>",
        "        </header>",
        "",
        "        <!-- Main Content -->",
        '        <main class="max-w-7xl mx-auto px-4 py-8">',
    ]

    # Render each category
    for category in categories:
        category_id = (
            category.title.replace(" ", "_")
            .replace("📥", "")
            .replace("🌐", "")
            .replace("🔌", "")
            .replace("📋", "")
            .replace("📄", "")
            .replace("⚠️", "")
            .strip()
        )

        html_parts.append(f"            <!-- {category.title} -->")
        html_parts.append(
            f'            <section id="{category_id}" class="mb-8" hx-get="/api/category/{category_id}" hx-trigger="every 2s" hx-swap="innerHTML">'
        )

        # Initial render
        category_html = render_category_section(category, metrics)
        html_parts.append(category_html)

        html_parts.append("            </section>")

    # Add pipeline stats/errors section
    html_parts.append("            <!-- Pipeline Stats Section -->")
    html_parts.append(
        '            <section id="pipeline_stats_section" class="mt-12 mb-8" hx-get="/api/pipeline-stats" hx-trigger="every 2s" hx-swap="innerHTML">'
    )
    html_parts.append(render_pipeline_stats_section())
    html_parts.append("            </section>")

    # Add log files section
    html_parts.append("            <!-- Log Files Section -->")
    html_parts.append(
        '            <section id="log_files_section" class="mt-12 mb-8" hx-get="/api/log-files" hx-trigger="every 2s" hx-swap="innerHTML">'
    )
    html_parts.append(render_log_files_section())
    html_parts.append("            </section>")

    html_parts.extend(
        [
            "        </main>",
            "",
            "        <!-- Footer -->",
            '        <footer class="bg-white border-t border-gray-200 mt-12">',
            '            <div class="max-w-7xl mx-auto px-4 py-6 text-center text-gray-500 text-sm">',
            "                <p>Dashboard auto-refreshes every 2 seconds</p>",
            "            </div>",
            "        </footer>",
            "    </div>",
            "</body>",
            "</html>",
        ]
    )

    return "\n".join(html_parts)


def render_pipeline_stats_section() -> str:
    """Render the pipeline statistics and errors section."""
    stats = get_stats()

    html_parts = [
        '                <div class="bg-white rounded-lg shadow-md p-6">',
        '                    <h2 class="text-2xl font-bold text-gray-800 mb-4">🔍 Pipeline Statistics & Errors</h2>',
    ]

    if not stats:
        html_parts.append(
            '                    <p class="text-gray-500">No pipeline statistics available yet.</p>'
        )
    else:
        # Check if there are any errors or warnings
        pipelines_with_errors = {name: st for name, st in stats.items() if st.get("errors")}
        pipelines_with_zero_results = {
            name: st
            for name, st in stats.items()
            if st.get("result_count", 0) == 0 and st.get("rows_matched", 0) == 0
        }

        # Show errors first if any
        if pipelines_with_errors:
            html_parts.append(
                '                    <div class="mb-6 bg-red-50 border border-red-200 rounded-lg p-4">'
            )
            html_parts.append(
                '                        <h3 class="text-lg font-semibold text-red-800 mb-3">⚠️ Pipeline Errors</h3>'
            )

            for pipeline_name, pipeline_stats in pipelines_with_errors.items():
                errors = pipeline_stats.get("errors", [])
                if errors:
                    html_parts.append('                        <div class="mb-3 last:mb-0">')
                    html_parts.append(
                        f'                            <h4 class="font-semibold text-red-700 mb-1">{pipeline_name}</h4>'
                    )
                    html_parts.append(
                        '                            <ul class="list-disc list-inside space-y-1">'
                    )
                    for error in errors[:10]:  # Show max 10 errors per pipeline
                        from html import escape

                        html_parts.append(
                            f'                                <li class="text-sm text-red-600 font-mono">{escape(error)}</li>'
                        )
                    if len(errors) > 10:
                        html_parts.append(
                            f'                                <li class="text-sm text-red-500 italic">... and {len(errors) - 10} more errors</li>'
                        )
                    html_parts.append("                            </ul>")
                    html_parts.append("                        </div>")

            html_parts.append("                    </div>")

        # Show warning for pipelines with zero results
        if pipelines_with_zero_results:
            html_parts.append(
                '                    <div class="mb-6 bg-yellow-50 border border-yellow-200 rounded-lg p-4">'
            )
            html_parts.append(
                '                        <h3 class="text-lg font-semibold text-yellow-800 mb-3">⚠️ Pipelines With No Results</h3>'
            )
            html_parts.append(
                '                        <p class="text-sm text-yellow-700 mb-2">These pipelines processed logs but produced no results. This may indicate:</p>'
            )
            html_parts.append(
                '                        <ul class="list-disc list-inside text-sm text-yellow-700 mb-3 ml-4">'
            )
            html_parts.append("                            <li>No matching events in the logs</li>")
            html_parts.append("                            <li>Filters are too strict</li>")
            html_parts.append(
                "                            <li>Required fields are missing from log entries</li>"
            )
            html_parts.append("                        </ul>")
            html_parts.append('                        <div class="space-y-2">')

            for pipeline_name, pipeline_stats in pipelines_with_zero_results.items():
                rows_processed = pipeline_stats.get("rows_processed", 0)
                rows_filtered = pipeline_stats.get("rows_filtered", 0)
                html_parts.append(
                    '                            <div class="bg-yellow-100 rounded p-2">'
                )
                html_parts.append(
                    f'                                <span class="font-semibold text-yellow-900">{pipeline_name}</span>'
                )
                html_parts.append(
                    f'                                <span class="text-yellow-700 text-sm ml-2">(Processed: {rows_processed}, Filtered: {rows_filtered})</span>'
                )
                html_parts.append("                            </div>")

            html_parts.append("                        </div>")
            html_parts.append("                    </div>")

        # Show detailed stats table
        html_parts.append('                    <div class="overflow-x-auto">')
        html_parts.append(
            '                        <table class="min-w-full divide-y divide-gray-200">'
        )
        html_parts.append('                            <thead class="bg-gray-50">')
        html_parts.append("                                <tr>")
        html_parts.append(
            '                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Pipeline</th>'
        )
        html_parts.append(
            '                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Processed</th>'
        )
        html_parts.append(
            '                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Matched</th>'
        )
        html_parts.append(
            '                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Filtered</th>'
        )
        html_parts.append(
            '                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Results</th>'
        )
        html_parts.append(
            '                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Errors</th>'
        )
        html_parts.append("                                </tr>")
        html_parts.append("                            </thead>")
        html_parts.append(
            '                            <tbody class="bg-white divide-y divide-gray-200">'
        )

        for pipeline_name, pipeline_stats in stats.items():
            rows_processed = pipeline_stats.get("rows_processed", 0)
            rows_matched = pipeline_stats.get("rows_matched", 0)
            rows_filtered = pipeline_stats.get("rows_filtered", 0)
            result_count = pipeline_stats.get("result_count", 0)
            error_count = len(pipeline_stats.get("errors", []))

            # Determine row color based on status
            row_class = ""
            if error_count > 0:
                row_class = "bg-red-50"
            elif result_count == 0 and rows_matched == 0:
                row_class = "bg-yellow-50"

            html_parts.append(f'                                <tr class="{row_class}">')
            html_parts.append(
                f'                                    <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{pipeline_name}</td>'
            )
            html_parts.append(
                f'                                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-700">{rows_processed:,}</td>'
            )
            html_parts.append(
                f'                                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-700">{rows_matched:,}</td>'
            )
            html_parts.append(
                f'                                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{rows_filtered:,}</td>'
            )

            # Results column with color coding
            if result_count > 0:
                result_class = "text-green-600 font-semibold"
            else:
                result_class = "text-gray-400"
            html_parts.append(
                f'                                    <td class="px-6 py-4 whitespace-nowrap text-sm {result_class}">{result_count:,}</td>'
            )

            # Errors column with color coding
            if error_count > 0:
                error_class = "text-red-600 font-semibold"
            else:
                error_class = "text-gray-400"
            html_parts.append(
                f'                                    <td class="px-6 py-4 whitespace-nowrap text-sm {error_class}">{error_count}</td>'
            )

            html_parts.append("                                </tr>")

        html_parts.append("                            </tbody>")
        html_parts.append("                        </table>")
        html_parts.append("                    </div>")

    html_parts.append("                </div>")

    return "\n".join(html_parts)


def render_log_files_section() -> str:
    """Render the log files section."""
    log_files = get_log_files()

    html_parts = [
        '                <div class="bg-white rounded-lg shadow-md p-6">',
        '                    <h2 class="text-2xl font-bold text-gray-800 mb-4">📄 Loaded Log Files</h2>',
    ]

    if not log_files:
        html_parts.append(
            '                    <p class="text-gray-500">No log files loaded yet.</p>'
        )
    else:
        html_parts.append('                    <div class="overflow-x-auto">')
        html_parts.append(
            '                        <table class="min-w-full divide-y divide-gray-200">'
        )
        html_parts.append('                            <thead class="bg-gray-50">')
        html_parts.append("                                <tr>")
        html_parts.append(
            '                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">File Name</th>'
        )
        html_parts.append(
            '                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Size</th>'
        )
        html_parts.append(
            '                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Progress</th>'
        )
        html_parts.append(
            '                                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>'
        )
        html_parts.append("                                </tr>")
        html_parts.append("                            </thead>")
        html_parts.append(
            '                            <tbody class="bg-white divide-y divide-gray-200">'
        )

        for log_file in log_files:
            file_name = log_file["name"]
            file_size = log_file["size"]
            position = log_file["position"]

            # Format file size
            if file_size < 1024:
                size_str = f"{file_size} B"
            elif file_size < 1024 * 1024:
                size_str = f"{file_size / 1024:.1f} KB"
            else:
                size_str = f"{file_size / (1024 * 1024):.1f} MB"

            # Calculate progress
            progress_pct = (position / file_size * 100) if file_size > 0 else 0

            html_parts.append("                                <tr>")
            html_parts.append(
                f'                                    <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{file_name}</td>'
            )
            html_parts.append(
                f'                                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{size_str}</td>'
            )
            html_parts.append(
                '                                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">'
            )
            html_parts.append(
                '                                        <div class="w-full bg-gray-200 rounded-full h-2.5">'
            )
            html_parts.append(
                f'                                            <div class="bg-blue-600 h-2.5 rounded-full" style="width: {progress_pct:.1f}%"></div>'
            )
            html_parts.append("                                        </div>")
            html_parts.append(
                f'                                        <span class="text-xs">{progress_pct:.1f}%</span>'
            )
            html_parts.append("                                    </td>")
            html_parts.append(
                '                                    <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">'
            )
            html_parts.append(
                f'                                        <a href="/api/log-file/{file_name}" target="_blank" class="text-indigo-600 hover:text-indigo-900">View</a>'
            )
            html_parts.append("                                    </td>")
            html_parts.append("                                </tr>")

        html_parts.append("                            </tbody>")
        html_parts.append("                        </table>")
        html_parts.append("                    </div>")

    html_parts.append("                </div>")

    return "\n".join(html_parts)


def render_category_section(category: Any, metrics: dict[str, dict[str, dict]]) -> str:
    """Render a category section with title and pipelines."""
    html_parts = [
        '                <div class="mb-6">',
        f'                    <h2 class="text-2xl font-bold text-gray-800 mb-4">{category.title}</h2>',
        '                    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">',
    ]

    # Render the category with HTML mode and stats
    stats = get_stats()
    category_html = category.render(metrics, mode="html", stats=stats)

    # Wrap in grid layout
    html_parts.append(
        "                        " + category_html.replace("\n", "\n                        ")
    )

    html_parts.extend(
        [
            "                    </div>",
            "                </div>",
        ]
    )

    return "\n".join(html_parts)


@app.get("/api/category/{category_name}", response_class=HTMLResponse)
async def get_category(category_name: str) -> str:
    """Get HTML for a specific category (for HTMX polling)."""
    _, categories = create_default_dashboard()
    metrics = get_metrics()

    # Find the matching category
    for category in categories:
        category_id = (
            category.title.replace(" ", "_")
            .replace("📥", "")
            .replace("🌐", "")
            .replace("🔌", "")
            .replace("📋", "")
            .replace("📄", "")
            .replace("⚠️", "")
            .strip()
        )
        if category_id == category_name:
            return render_category_section(category, metrics)

    return '<div class="text-red-500">Category not found</div>'


@app.get("/api/pipeline-stats", response_class=HTMLResponse)
async def get_pipeline_stats_section() -> str:
    """Get HTML for pipeline stats section (for HTMX polling)."""
    return render_pipeline_stats_section()


@app.get("/api/log-files", response_class=HTMLResponse)
async def get_log_files_section() -> str:
    """Get HTML for log files section (for HTMX polling)."""
    return render_log_files_section()


@app.get("/api/log-file/{file_name}", response_class=HTMLResponse)
async def view_log_file(file_name: str) -> str:
    """View a specific log file with syntax highlighting and line numbers."""
    import json
    import os
    from html import escape

    log_files = get_log_files()

    # Find the log file
    log_file_path = None
    for log_file in log_files:
        if log_file["name"] == file_name:
            log_file_path = log_file["path"]
            break

    if not log_file_path or not os.path.exists(log_file_path):
        return """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Log File Not Found</title>
            <script src="https://cdn.tailwindcss.com"></script>
        </head>
        <body class="bg-gray-50">
            <div class="max-w-7xl mx-auto px-4 py-8">
                <div class="bg-red-50 border border-red-200 rounded-lg p-6">
                    <h1 class="text-2xl font-bold text-red-800 mb-2">Log File Not Found</h1>
                    <p class="text-red-600">The requested log file could not be found.</p>
                    <a href="/" class="text-indigo-600 hover:text-indigo-900 mt-4 inline-block">← Back to Dashboard</a>
                </div>
            </div>
        </body>
        </html>
        """

    # Read the log file
    lines = []
    try:
        with open(log_file_path) as f:
            lines = f.readlines()
    except Exception as e:
        return f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Error Reading Log File</title>
            <script src="https://cdn.tailwindcss.com"></script>
        </head>
        <body class="bg-gray-50">
            <div class="max-w-7xl mx-auto px-4 py-8">
                <div class="bg-red-50 border border-red-200 rounded-lg p-6">
                    <h1 class="text-2xl font-bold text-red-800 mb-2">Error Reading Log File</h1>
                    <p class="text-red-600">Error: {escape(str(e))}</p>
                    <a href="/" class="text-indigo-600 hover:text-indigo-900 mt-4 inline-block">← Back to Dashboard</a>
                </div>
            </div>
        </body>
        </html>
        """

    # Build HTML
    html_parts = [
        "<!DOCTYPE html>",
        '<html lang="en">',
        "<head>",
        '    <meta charset="UTF-8">',
        '    <meta name="viewport" content="width=device-width, initial-scale=1.0">',
        f"    <title>Log Viewer - {escape(file_name)}</title>",
        '    <script src="https://cdn.tailwindcss.com"></script>',
        "    <style>",
        '        .log-line { font-family: "Monaco", "Menlo", "Ubuntu Mono", monospace; font-size: 12px; }',
        "        .log-line:hover { background-color: #f3f4f6; }",
        "        .line-number { user-select: none; color: #9ca3af; }",
        "        .json-key { color: #059669; }",
        "        .json-string { color: #d97706; }",
        "        .json-number { color: #2563eb; }",
        "        .json-boolean { color: #7c3aed; }",
        "        .json-null { color: #dc2626; }",
        "    </style>",
        "</head>",
        '<body class="bg-gray-50">',
        '    <div class="min-h-screen">',
        '        <header class="bg-gradient-to-r from-indigo-600 to-purple-600 text-white shadow-lg sticky top-0 z-10">',
        '            <div class="max-w-7xl mx-auto px-4 py-4">',
        '                <div class="flex items-center justify-between">',
        "                    <div>",
        f'                        <h1 class="text-2xl font-bold">Log Viewer: {escape(file_name)}</h1>',
        f'                        <p class="text-indigo-100 text-sm mt-1">{len(lines):,} lines</p>',
        "                    </div>",
        '                    <a href="/" class="bg-white text-indigo-600 px-4 py-2 rounded-lg hover:bg-indigo-50 transition">← Back to Dashboard</a>',
        "                </div>",
        "            </div>",
        "        </header>",
        '        <main class="max-w-7xl mx-auto px-4 py-8">',
        '            <div class="bg-white rounded-lg shadow-md overflow-hidden">',
        '                <div class="p-4 bg-gray-50 border-b border-gray-200">',
        '                    <div class="flex items-center space-x-4">',
        '                        <span class="text-sm text-gray-600">Total Lines: <strong>'
        + f"{len(lines):,}"
        + "</strong></span>",
        "                    </div>",
        "                </div>",
        '                <div class="overflow-x-auto">',
        '                    <table class="w-full">',
        "                        <tbody>",
    ]

    # Process each line
    for line_num, line in enumerate(lines, 1):
        line_content = line.rstrip("\n")

        # Try to parse as JSON for syntax highlighting
        formatted_line = escape(line_content)
        try:
            parsed = json.loads(line_content)
            # Format JSON with syntax highlighting
            formatted_json = json.dumps(parsed, indent=2)
            formatted_line = syntax_highlight_json(formatted_json)
        except json.JSONDecodeError:
            # Not JSON, just escape HTML
            pass

        html_parts.append(
            '                            <tr class="log-line border-b border-gray-100">'
        )
        html_parts.append(
            f'                                <td class="line-number px-4 py-1 text-right border-r border-gray-200 bg-gray-50" style="min-width: 60px;">{line_num}</td>'
        )
        html_parts.append(
            f'                                <td class="px-4 py-1"><pre class="whitespace-pre-wrap break-all m-0">{formatted_line}</pre></td>'
        )
        html_parts.append("                            </tr>")

    html_parts.extend(
        [
            "                        </tbody>",
            "                    </table>",
            "                </div>",
            "            </div>",
            "        </main>",
            "    </div>",
            "</body>",
            "</html>",
        ]
    )

    return "\n".join(html_parts)


def syntax_highlight_json(json_str: str) -> str:
    """Apply basic syntax highlighting to JSON string."""
    import re
    from html import escape

    # Escape HTML first
    highlighted = escape(json_str)

    # Highlight different JSON elements
    # Keys (quoted strings followed by :)
    highlighted = re.sub(r'"([^"]+)"\s*:', r'<span class="json-key">"\1"</span>:', highlighted)

    # String values
    highlighted = re.sub(r':\s*"([^"]*)"', r': <span class="json-string">"\1"</span>', highlighted)

    # Numbers
    highlighted = re.sub(r"\b(\d+\.?\d*)\b", r'<span class="json-number">\1</span>', highlighted)

    # Booleans
    highlighted = re.sub(r"\b(true|false)\b", r'<span class="json-boolean">\1</span>', highlighted)

    # Null
    highlighted = re.sub(r"\bnull\b", r'<span class="json-null">null</span>', highlighted)

    return highlighted


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}
