"""
Renderers for displaying analytics pipeline results.

Renderers are responsible for formatting pipeline results into various output formats
(terminal text, HTML, etc.). This decouples display logic from the analytics pipeline.
"""

from abc import ABC, abstractmethod


class Renderer(ABC):
    """Base renderer interface for displaying pipeline results."""

    @abstractmethod
    def render(self, results: dict[str, dict]) -> str:
        """
        Render pipeline results to a string.

        Args:
            results: Dictionary mapping group keys to aggregated data
                    e.g., {"host1": {"_average": 2.3, "_average_n": 10}}

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

    def render(self, results: dict[str, dict]) -> str:
        if not results:
            return "No data"

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

    def render(self, results: dict[str, dict]) -> str:
        if not results:
            return "No data"

        lines = []
        # Sort by count (most data first)
        sorted_items = sorted(
            results.items(), key=lambda x: x[1].get(self.count_field, 0), reverse=True
        )

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

    def render(self, results: dict[str, dict]) -> str:
        if not results:
            return "0 total"

        total = sum(data.get(self.count_field, 0) for data in results.values())
        lines = [f"{total} total"]

        if self.show_breakdown and len(results) > 1:
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

    def render(self, results: dict[str, dict]) -> str:
        if not results:
            return "No data"

        lines = []
        sorted_items = sorted(
            results.items(), key=lambda x: x[1].get(self.total_field, 0), reverse=True
        )

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

    def render(self, results: dict[str, dict]) -> str:
        if not results:
            return "0 open connections"

        total_open = sum(data.get(self.connections_field, 0) for data in results.values())
        lines = [f"{total_open} open connections"]

        # Show only hosts with open connections
        active_hosts = {
            key: data for key, data in results.items() if data.get(self.connections_field, 0) > 0
        }

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
    ):
        """
        Args:
            title: Category title (can include emoji)
            pipelines: List of (pipeline_name, renderer) tuples
            width: Width of the separator line
        """
        self.title = title
        self.pipelines = pipelines
        self.width = width

    def render(self, all_results: dict[str, dict[str, dict]]) -> str:
        """
        Render all pipelines in this category.

        Args:
            all_results: Dict mapping pipeline names to their results

        Returns:
            Formatted category string
        """
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

        return "\n".join(lines)
