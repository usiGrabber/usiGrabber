"""
Renderers for displaying analytics pipeline results.

Renderers are responsible for formatting pipeline results into various output formats
(terminal text, HTML, etc.). This decouples display logic from the analytics pipeline.
"""

from abc import ABC, abstractmethod
from typing import Literal


class Renderer(ABC):
    """Base renderer interface for displaying pipeline results."""

    @abstractmethod
    def render(self, results: dict[str, dict], mode: Literal["text", "html"] = "text") -> str:
        """
        Render pipeline results to a string.

        Args:
            results: Dictionary mapping group keys to aggregated data
                    e.g., {"host1": {"_average": 2.3, "_average_n": 10}}
            mode: Output mode - "text" for terminal output, "html" for HTML output

        Returns:
            Formatted string representation
        """
        pass


class SimpleTextRenderer(Renderer):
    """
    Simple text renderer that displays key-value pairs.

    Example output:
        host1: 5
        host2: 3
    """

    def __init__(self, value_field: str = "_count", label: str = ""):
        """
        Args:
            value_field: Which field to display from the aggregated data
            label: Optional label to prepend to values
        """
        self.value_field = value_field
        self.label = label

    def render(self, results: dict[str, dict], mode: Literal["text", "html"] = "text") -> str:
        if not results:
            if mode == "html":
                return '<div class="text-gray-500 italic">No data</div>'
            return "No data"

        if mode == "html":
            html_lines = ['<div class="space-y-1">']
            for key, data in results.items():
                value = data.get(self.value_field, 0)
                label_text = f"{self.label} " if self.label else ""
                html_lines.append(
                    f'<div class="flex justify-between">'
                    f'<span class="text-gray-700">{key}</span>'
                    f'<span class="font-semibold">{label_text}{value}</span>'
                    f"</div>"
                )
            html_lines.append("</div>")
            return "\n".join(html_lines)

        lines = []
        for key, data in results.items():
            value = data.get(self.value_field, 0)
            if self.label:
                lines.append(f"{key}: {self.label} {value}")
            else:
                lines.append(f"{key}: {value}")

        return "\n".join(lines)


class AverageRenderer(Renderer):
    """
    Renders average values with color coding for terminal output.

    Example output:
        ftp.pride.ebi.ac.uk: 2.34s (n=156)
        www.ebi.ac.uk: 0.45s (n=23)
    """

    def __init__(
        self,
        average_field: str = "_average",
        count_field: str = "_average_n",
        unit: str = "s",
        decimals: int = 2,
        use_colors: bool = True,
        max_items: int = 5,
    ):
        self.average_field = average_field
        self.count_field = count_field
        self.unit = unit
        self.decimals = decimals
        self.use_colors = use_colors
        self.max_items = max_items

    def render(self, results: dict[str, dict], mode: Literal["text", "html"] = "text") -> str:
        if not results:
            if mode == "html":
                return '<div class="text-gray-500 italic">No data</div>'
            return "No data"

        # Sort by count (most data first)
        sorted_items = sorted(
            results.items(), key=lambda x: x[1].get(self.count_field, 0), reverse=True
        )

        if mode == "html":
            html_lines = ['<div class="space-y-2">']
            for key, data in sorted_items[: self.max_items]:
                avg = data.get(self.average_field, 0)
                count = data.get(self.count_field, 0)
                value_str = f"{avg:.{self.decimals}f}{self.unit}"

                html_lines.append(
                    f'<div class="flex justify-between items-center">'
                    f'<span class="text-gray-700 text-sm">{key}</span>'
                    f'<div class="text-right">'
                    f'<span class="font-semibold text-blue-600">{value_str}</span>'
                    f'<span class="text-gray-500 text-xs ml-2">(n={count})</span>'
                    f"</div>"
                    f"</div>"
                )
            html_lines.append("</div>")
            return "\n".join(html_lines)

        lines = []
        for key, data in sorted_items[: self.max_items]:
            avg = data.get(self.average_field, 0)
            count = data.get(self.count_field, 0)

            value_str = f"{avg:.{self.decimals}f}{self.unit}"
            lines.append(f"{key}: {value_str} (n={count})")

        return "\n".join(lines)


class CountRenderer(Renderer):
    """
    Renders count values.

    Shows total count and optionally a breakdown by key.
    """

    def __init__(
        self,
        count_field: str = "_count",
        show_breakdown: bool = True,
        max_items: int = 10,
    ):
        self.count_field = count_field
        self.show_breakdown = show_breakdown
        self.max_items = max_items

    def render(self, results: dict[str, dict], mode: Literal["text", "html"] = "text") -> str:
        if not results:
            if mode == "html":
                return '<div class="text-sm text-gray-400 italic">No data</div>'
            return "No data"

        total = sum(data.get(self.count_field, 0) for data in results.values())

        # Handle zero total with data (shouldn't happen, but be defensive)
        if total == 0:
            if mode == "html":
                return '<div class="text-sm text-gray-400 italic">No data</div>'
            return "No data"

        if mode == "html":
            html_lines = [
                f'<div class="text-3xl font-bold text-indigo-600">{total}</div>',
                '<div class="text-sm text-gray-500 mb-3">total</div>',
            ]

            if self.show_breakdown and len(results) >= 1:
                sorted_items = sorted(
                    results.items(), key=lambda x: x[1].get(self.count_field, 0), reverse=True
                )

                html_lines.append('<div class="space-y-1 mt-2 pt-2 border-t border-gray-200">')
                for key, data in sorted_items[: self.max_items]:
                    count = data.get(self.count_field, 0)
                    html_lines.append(
                        f'<div class="flex justify-between text-sm">'
                        f'<span class="text-gray-600">{key}</span>'
                        f'<span class="font-semibold text-gray-700">{count}</span>'
                        f"</div>"
                    )
                html_lines.append("</div>")

            return "\n".join(html_lines)

        lines = [f"{total} total"]

        if self.show_breakdown and len(results) >= 1:
            sorted_items = sorted(
                results.items(), key=lambda x: x[1].get(self.count_field, 0), reverse=True
            )

            for key, data in sorted_items[: self.max_items]:
                count = data.get(self.count_field, 0)
                lines.append(f"  {key}: {count}")

        return "\n".join(lines)


class ErrorRateRenderer(Renderer):
    """
    Renders error rates with color coding.

    Green: < 5%
    Yellow: 5-15%
    Red: > 15%
    """

    def __init__(
        self,
        error_rate_field: str = "_error_rate",
        total_field: str = "_total",
        errors_field: str = "_errors",
        use_colors: bool = True,
        max_items: int = 5,
    ):
        self.error_rate_field = error_rate_field
        self.total_field = total_field
        self.errors_field = errors_field
        self.use_colors = use_colors
        self.max_items = max_items

    def _get_color(self, error_rate: float) -> str:
        """Get ANSI color code based on error rate."""
        if not self.use_colors:
            return ""

        if error_rate < 5:
            return "\033[92m"  # Green
        elif error_rate < 15:
            return "\033[93m"  # Yellow
        else:
            return "\033[91m"  # Red

    def _get_html_color_class(self, error_rate: float) -> str:
        """Get Tailwind color class based on error rate."""
        if error_rate < 5:
            return "text-green-600"
        elif error_rate < 15:
            return "text-yellow-600"
        else:
            return "text-red-600"

    def render(self, results: dict[str, dict], mode: Literal["text", "html"] = "text") -> str:
        if not results:
            if mode == "html":
                return '<div class="text-gray-500 italic">No data</div>'
            return "No data"

        sorted_items = sorted(
            results.items(), key=lambda x: x[1].get(self.total_field, 0), reverse=True
        )

        if mode == "html":
            html_lines = ['<div class="space-y-2">']
            for key, data in sorted_items[: self.max_items]:
                error_rate = data.get(self.error_rate_field, 0)
                total = data.get(self.total_field, 0)
                errors = data.get(self.errors_field, 0)

                color_class = self._get_html_color_class(error_rate)
                html_lines.append(
                    f'<div class="flex justify-between items-center">'
                    f'<span class="text-gray-700 text-sm">{key}</span>'
                    f'<div class="text-right">'
                    f'<span class="font-semibold {color_class}">{error_rate:.1f}%</span>'
                    f'<span class="text-gray-500 text-xs ml-2">({errors}/{total})</span>'
                    f"</div>"
                    f"</div>"
                )
            html_lines.append("</div>")
            return "\n".join(html_lines)

        lines = []
        for key, data in sorted_items[: self.max_items]:
            error_rate = data.get(self.error_rate_field, 0)
            total = data.get(self.total_field, 0)
            errors = data.get(self.errors_field, 0)

            color = self._get_color(error_rate)
            reset = "\033[0m" if self.use_colors else ""

            lines.append(f"{key}: {color}{error_rate:.1f}%{reset} ({errors}/{total})")

        return "\n".join(lines)


class OpenConnectionsRenderer(Renderer):
    """
    Renders current open connections count.

    Shows total and breakdown by host.
    """

    def __init__(
        self,
        connections_field: str = "_open_connections",
        max_items: int = 5,
    ):
        self.connections_field = connections_field
        self.max_items = max_items

    def render(self, results: dict[str, dict], mode: Literal["text", "html"] = "text") -> str:
        if not results:
            if mode == "html":
                return '<div class="text-sm text-gray-400 italic">No data</div>'
            return "No data"

        total_open = sum(data.get(self.connections_field, 0) for data in results.values())

        # Show only hosts with open connections
        active_hosts = {
            key: data for key, data in results.items() if data.get(self.connections_field, 0) > 0
        }

        if mode == "html":
            html_lines = [
                f'<div class="text-3xl font-bold text-green-600">{total_open}</div>',
                '<div class="text-sm text-gray-500 mb-3">open connections</div>',
            ]

            if active_hosts:
                sorted_items = sorted(
                    active_hosts.items(),
                    key=lambda x: x[1].get(self.connections_field, 0),
                    reverse=True,
                )

                html_lines.append('<div class="space-y-1 mt-2 pt-2 border-t border-gray-200">')
                for key, data in sorted_items[: self.max_items]:
                    count = data.get(self.connections_field, 0)
                    html_lines.append(
                        f'<div class="flex justify-between text-sm">'
                        f'<span class="text-gray-600">{key}</span>'
                        f'<span class="font-semibold text-gray-700">{count}</span>'
                        f"</div>"
                    )
                html_lines.append("</div>")

            return "\n".join(html_lines)

        lines = [f"{total_open} open connections"]

        if active_hosts:
            sorted_items = sorted(
                active_hosts.items(),
                key=lambda x: x[1].get(self.connections_field, 0),
                reverse=True,
            )

            for key, data in sorted_items[: self.max_items]:
                count = data.get(self.connections_field, 0)
                lines.append(f"  {key}: {count}")

        return "\n".join(lines)


class SessionMemoryRenderer(Renderer):
    """
    Renders SQLAlchemy session identity map size to track memory usage.

    Shows current identity map sizes for each project being processed.
    """

    def __init__(
        self,
        size_field: str = "identity_map_size",
        max_items: int = 10,
    ):
        self.size_field = size_field
        self.max_items = max_items

    def render(self, results: dict[str, dict], mode: Literal["text", "html"] = "text") -> str:
        if not results:
            if mode == "html":
                return '<div class="text-gray-500 italic">No data</div>'
            return "No data"

        # Sort by size (largest first to spot memory issues)
        sorted_items = sorted(
            results.items(), key=lambda x: x[1].get(self.size_field, 0), reverse=True
        )

        if mode == "html":
            html_lines = ['<div class="space-y-1">']
            for key, data in sorted_items[: self.max_items]:
                size = data.get(self.size_field, 0)

                # Color code based on size (larger = more concerning)
                if size > 10000:
                    color_class = "text-red-600 font-bold"
                elif size > 5000:
                    color_class = "text-yellow-600 font-semibold"
                else:
                    color_class = "text-green-600"

                html_lines.append(
                    f'<div class="flex justify-between text-sm">'
                    f'<span class="text-gray-600 truncate">{key}</span>'
                    f'<span class="{color_class}">{size:,} objects</span>'
                    f"</div>"
                )
            html_lines.append("</div>")
            return "\n".join(html_lines)

        lines = []
        for key, data in sorted_items[: self.max_items]:
            size = data.get(self.size_field, 0)
            lines.append(f"{key}: {size:,} objects")

        return "\n".join(lines)


class DatabasePoolRenderer(Renderer):
    """
    Renders database connection pool status.

    Shows pool size, checked in/out connections, and overflow.
    """

    def __init__(self, max_items: int = 5):
        self.max_items = max_items

    def render(self, results: dict[str, dict], mode: Literal["text", "html"] = "text") -> str:
        if not results:
            if mode == "html":
                return '<div class="text-gray-500 italic">No data</div>'
            return "No data"

        # Get the most recent entry (assuming keys are timestamps or similar)
        sorted_items = sorted(results.items(), reverse=True)

        if mode == "html":
            html_lines = ['<div class="space-y-2">']
            for key, data in sorted_items[: self.max_items]:
                pool_size = data.get("pool_size", 0)
                checked_in = data.get("checked_in", 0)
                checked_out = data.get("checked_out", 0)
                overflow = data.get("overflow", 0)

                # Color code overflow (red if overflowing)
                overflow_class = "text-red-600 font-bold" if overflow > 0 else "text-gray-600"

                html_lines.append(
                    f'<div class="border-l-4 border-blue-500 pl-3 py-1">'
                    f'<div class="text-xs text-gray-500 mb-1">{key}</div>'
                    f'<div class="grid grid-cols-2 gap-2 text-sm">'
                    f'<div><span class="text-gray-600">Size:</span> <span class="font-semibold">{pool_size}</span></div>'
                    f'<div><span class="text-gray-600">In:</span> <span class="font-semibold text-green-600">{checked_in}</span></div>'
                    f'<div><span class="text-gray-600">Out:</span> <span class="font-semibold text-blue-600">{checked_out}</span></div>'
                    f'<div><span class="text-gray-600">Overflow:</span> <span class="{overflow_class}">{overflow}</span></div>'
                    f"</div>"
                    f"</div>"
                )
            html_lines.append("</div>")
            return "\n".join(html_lines)

        lines = []
        for key, data in sorted_items[: self.max_items]:
            pool_size = data.get("pool_size", 0)
            checked_in = data.get("checked_in", 0)
            checked_out = data.get("checked_out", 0)
            overflow = data.get("overflow", 0)

            lines.append(f"{key}:")
            lines.append(
                f"  Pool size: {pool_size}, In: {checked_in}, Out: {checked_out}, Overflow: {overflow}"
            )

        return "\n".join(lines)


class CategoryRenderer:
    """
    Groups multiple pipelines under a category with a title and bullet points.

    Example output:
        📥 FTP Downloads
        ────────────────────────────────────
        • avg_response_time:
          ftp.pride.ebi.ac.uk: 2.34s (n=156)
        • error_rate:
          ftp.pride.ebi.ac.uk: 3.2% (5/156)
    """

    def __init__(
        self,
        title: str,
        pipelines: list[tuple[str, Renderer]],
        width: int = 100,
        stats: dict[str, dict[str, int | list[str]]] | None = None,
    ):
        """
        Args:
            title: Category title (can include emoji)
            pipelines: List of (pipeline_name, renderer) tuples
            width: Width of the separator line
            stats: Optional pipeline statistics for error detection
        """
        self.title = title
        self.pipelines = pipelines
        self.width = width
        self.stats = stats or {}

    def render(
        self,
        all_results: dict[str, dict[str, dict]],
        mode: Literal["text", "html"] = "text",
        stats: dict[str, dict[str, int | list[str]]] | None = None,
    ) -> str:
        """
        Render all pipelines in this category.

        Args:
            all_results: Dict mapping pipeline names to their results
            mode: Output mode - "text" for terminal output, "html" for HTML output

        Returns:
            Formatted category string
        """
        # Use provided stats or fall back to instance stats
        pipeline_stats = stats or self.stats

        if mode == "html":
            html_lines = []

            for pipeline_name, renderer in self.pipelines:
                results = all_results.get(pipeline_name, {})
                rendered = renderer.render(results, mode="html")

                # Check for errors
                error_html = ""
                if pipeline_name in pipeline_stats:
                    errors = pipeline_stats[pipeline_name].get("errors", [])
                    if errors:
                        from html import escape

                        error_html = (
                            f'<div class="mt-2 p-2 bg-red-50 border border-red-200 rounded text-xs">'
                            f'<span class="text-red-700 font-semibold">⚠ Error:</span> '
                            f'<span class="text-red-600">{escape(errors[0][:100])}...</span>'
                            f"</div>"
                        )

                html_lines.append(
                    f'<div class="bg-white rounded-lg shadow p-4 mb-4">'
                    f'<h3 class="text-sm font-semibold text-gray-600 uppercase tracking-wide mb-3">{pipeline_name}</h3>'
                    f"{rendered}"
                    f"{error_html}"
                    f"</div>"
                )

            return "\n".join(html_lines)

        lines = []
        lines.append(self.title)
        lines.append("─" * self.width)

        for pipeline_name, renderer in self.pipelines:
            results = all_results.get(pipeline_name, {})

            # Render with bullet point
            rendered = renderer.render(results)

            # Add bullet and indent continuation lines
            rendered_lines = rendered.split("\n")
            if rendered_lines:
                lines.append(f"• {pipeline_name}:")
                for line in rendered_lines:
                    lines.append(f"  {line}")

                # Add error message if present
                if pipeline_name in pipeline_stats:
                    errors = pipeline_stats[pipeline_name].get("errors", [])
                    if errors:
                        lines.append(f"  \033[91m⚠ Error: {errors[0][:80]}...\033[0m")

        return "\n".join(lines)
