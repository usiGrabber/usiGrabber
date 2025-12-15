"""PSM persistence performance analyzer with histogram visualization."""

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

app_cli = typer.Typer(help="PSM persistence performance analyzer")


@dataclass
class PSMEntry:
    """Parsed PSM log entry."""

    timestamp: datetime
    psm_count: int
    persist_time: float
    parsing_time: float
    total_time: float
    project_accession: str
    file_name: str
    task_name: str

    @property
    def psm_per_second(self) -> float:
        """Calculate PSMs per second for persistence."""
        return self.psm_count / self.persist_time if self.persist_time > 0 else 0


class PSMAnalyzer:
    """Analyzes PSM import performance from log files."""

    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        self.entries: list[PSMEntry] = []
        self.first_timestamp: datetime | None = None
        self.last_timestamp: datetime | None = None
        self.num_log_files: int = 0

    def parse_logs(self) -> None:
        """Parse all JSONL files in the directory."""
        jsonl_files = sorted(self.log_dir.glob("*.jsonl"))
        self.num_log_files = len(jsonl_files)

        for jsonl_file in jsonl_files:
            with open(jsonl_file) as f:
                for line in f:
                    try:
                        data = json.loads(line.strip())
                        if "duration" in data and "message" in data:
                            entry = self._parse_entry(data)
                            if entry:
                                self.entries.append(entry)
                                self._update_timestamps(entry.timestamp)
                    except (json.JSONDecodeError, ValueError, KeyError):
                        continue

    def _parse_entry(self, data: dict[str, Any]) -> PSMEntry | None:
        """Parse a single log entry and extract PSM count."""
        message = data.get("message", "")

        match = re.search(r"Imported ([\d,]+) PSMs from", message)
        if not match:
            return None

        psm_count = int(match.group(1).replace(",", ""))
        duration = data["duration"]

        iso_timestamp = data.get("iso_timestamp") or data.get("timestamp")
        timestamp = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))

        return PSMEntry(
            timestamp=timestamp,
            psm_count=psm_count,
            persist_time=duration.get("persist", 0.0),
            parsing_time=duration.get("parsing", 0.0),
            total_time=duration.get("total", 0.0),
            project_accession=data.get("project_accession", ""),
            file_name=data.get("file_name", ""),
            task_name=data.get("taskName", ""),
        )

    def _update_timestamps(self, timestamp: datetime) -> None:
        if self.first_timestamp is None or timestamp < self.first_timestamp:
            self.first_timestamp = timestamp
        if self.last_timestamp is None or timestamp > self.last_timestamp:
            self.last_timestamp = timestamp

    def calculate_statistics(self, num_workers: int | None = None) -> dict[str, Any]:
        """Calculate comprehensive statistics accounting for parallel workers."""
        if not self.entries:
            return {}

        total_psms = sum(e.psm_count for e in self.entries)
        total_persist_time = sum(e.persist_time for e in self.entries)
        total_parsing_time = sum(e.parsing_time for e in self.entries)

        # Auto-detect workers: one log file per worker
        num_workers = self.num_log_files if self.num_log_files > 0 else 1

        if self.first_timestamp and self.last_timestamp:
            total_duration = (self.last_timestamp - self.first_timestamp).total_seconds()
        else:
            total_duration = 0

        theoretical_max_time = total_duration * num_workers
        persist_percentage = (
            (total_persist_time / theoretical_max_time * 100) if theoretical_max_time > 0 else 0
        )

        target_rate = 3000

        theoretical_persist_times = [e.psm_count / target_rate for e in self.entries]
        theoretical_total_persist_time = (
            sum(theoretical_persist_times) if theoretical_persist_times else 0
        )

        actual_sum_persist = sum(e.persist_time for e in self.entries) if self.entries else 0

        time_saved = actual_sum_persist - theoretical_total_persist_time
        theoretical_total_duration = total_duration - (time_saved / num_workers)
        speedup_factor = (
            actual_sum_persist / theoretical_total_persist_time
            if theoretical_total_duration > 0
            else 0
        )

        psm_rates = [e.psm_per_second for e in self.entries]

        return {
            "total_psms": total_psms,
            "total_entries": len(self.entries),
            "num_workers": num_workers,
            "total_persist_time": total_persist_time,
            "total_parsing_time": total_parsing_time,
            "total_duration": total_duration,
            "theoretical_max_time": theoretical_max_time,
            "persist_percentage": persist_percentage,
            "avg_persist_rate": total_psms / total_persist_time if total_persist_time > 0 else 0,
            "actual_max_persist": 0,
            "target_rate": target_rate,
            "theoretical_max_persist": 0,
            "time_saved": time_saved,
            "theoretical_total_duration": theoretical_total_duration,
            "speedup_factor": speedup_factor,
            "psm_rates": psm_rates,
            "first_timestamp": self.first_timestamp,
            "last_timestamp": self.last_timestamp,
        }

    def create_histogram(
        self,
        psm_rates: list[float],
        bins: int = 20,
        min_rate: float | None = None,
        max_rate: float | None = None,
    ) -> str:
        if not psm_rates:
            return "No data"

        if min_rate is None:
            min_rate = min(psm_rates)
        if max_rate is None:
            max_rate = max(psm_rates)

        bin_width = (max_rate - min_rate) / bins
        histogram = [0] * bins

        for rate in psm_rates:
            if rate < min_rate or rate > max_rate:
                continue
            if rate == max_rate:
                bin_idx = bins - 1
            else:
                bin_idx = int((rate - min_rate) / bin_width)
            histogram[bin_idx] += 1

        max_count = max(histogram)
        bar_width = 50

        lines = []
        lines.append("\nPSM/s Distribution Histogram:")
        lines.append("-" * 70)

        for i in range(bins):
            bin_start = min_rate + i * bin_width
            bin_end = bin_start + bin_width
            count = histogram[i]
            bar_length = int((count / max_count) * bar_width) if max_count > 0 else 0
            bar = "█" * bar_length

            lines.append(f"{bin_start:7.1f} - {bin_end:7.1f} PSM/s | {bar} {count}")

        return "\n".join(lines)


@app_cli.command()
def analyze(
    log_dir: Path = typer.Argument(
        ...,
        help="Directory containing JSONL log files",
        exists=True,
        file_okay=False,
        dir_okay=True,
    ),
    bins: int = typer.Option(20, help="Number of bins for histogram"),
    hist_min: float | None = typer.Option(None, help="Minimum value for histogram (default: auto)"),
    hist_max: float | None = typer.Option(None, help="Maximum value for histogram (default: auto)"),
) -> None:
    """Analyze PSM persistence performance from log files."""
    console = Console()

    with console.status("[bold green]Parsing log files..."):
        analyzer = PSMAnalyzer(log_dir)
        analyzer.parse_logs()

    # no workers option → always auto detect
    stats = analyzer.calculate_statistics()

    if not stats:
        console.print("[red]No PSM import entries found in log files.[/red]")
        return

    summary_table = Table(title="PSM Persistence Performance Summary", show_header=False)
    summary_table.add_column("Metric", style="cyan", no_wrap=True)
    summary_table.add_column("Value", style="yellow")

    summary_table.add_row("Total PSMs Imported", f"{stats['total_psms']:,}")
    summary_table.add_row("Total Import Operations", f"{stats['total_entries']:,}")
    summary_table.add_row("Parallel Workers", f"{stats['num_workers']}")
    summary_table.add_row("", "")

    summary_table.add_row("First Event", str(stats["first_timestamp"]))
    summary_table.add_row("Last Event", str(stats["last_timestamp"]))
    summary_table.add_row(
        "Total Duration (wall clock)",
        f"{stats['total_duration']:.1f}s ({stats['total_duration'] / 60:.1f} min)",
    )
    summary_table.add_row("", "")

    summary_table.add_row("Total Parsing Time (sum)", f"{stats['total_parsing_time']:.1f}s")
    summary_table.add_row("Total Persist Time (sum)", f"{stats['total_persist_time']:.1f}s")
    summary_table.add_row(
        "Available Worker Time",
        f"{stats['theoretical_max_time']:.1f}s ({stats['num_workers']} workers × {stats['total_duration']:.1f}s)",
    )
    summary_table.add_row("Persist % of Worker Time", f"{stats['persist_percentage']:.1f}%")
    summary_table.add_row("", "")

    summary_table.add_row("Average Persist Rate", f"{stats['avg_persist_rate']:.1f} PSM/s")
    summary_table.add_row("", "")

    summary_table.add_row(
        "[bold]Target Rate[/bold]", f"[bold]{stats['target_rate']:,} PSM/s[/bold]"
    )
    summary_table.add_row(
        "Time Saved (critical path)",
        f"{stats['time_saved']:.1f}s ({stats['time_saved'] / 60:.1f} min)",
    )
    summary_table.add_row(
        "Theoretical Total Duration",
        f"{stats['theoretical_total_duration']:.1f}s ({stats['theoretical_total_duration'] / 60:.1f} min)",
    )
    summary_table.add_row(
        "[bold green]Speedup Factor[/bold green]",
        f"[bold green]{stats['speedup_factor']:.2f}x[/bold green]",
    )

    console.print(summary_table)

    histogram = analyzer.create_histogram(
        stats["psm_rates"], bins=bins, min_rate=hist_min, max_rate=hist_max
    )
    console.print(histogram)

    console.print("\n[bold]PSM/s Statistics:[/bold]")
    console.print(f"  Min:    {min(stats['psm_rates']):.1f} PSM/s")
    console.print(f"  Max:    {max(stats['psm_rates']):.1f} PSM/s")
    console.print(f"  Median: {sorted(stats['psm_rates'])[len(stats['psm_rates']) // 2]:.1f} PSM/s")
    console.print(f"  Mean:   {sum(stats['psm_rates']) / len(stats['psm_rates']):.1f} PSM/s")


@app_cli.command()
def export(
    log_dir: Path = typer.Argument(
        ...,
        help="Directory containing JSONL log files",
        exists=True,
        file_okay=False,
        dir_okay=True,
    ),
    output: Path = typer.Option(
        Path("psm_analysis.csv"),
        help="Output CSV file",
    ),
) -> None:
    """Export PSM performance data to CSV for further analysis."""
    console = Console()

    with console.status("[bold green]Parsing log files..."):
        analyzer = PSMAnalyzer(log_dir)
        analyzer.parse_logs()

    if not analyzer.entries:
        console.print("[red]No PSM import entries found in log files.[/red]")
        return

    with open(output, "w") as f:
        f.write(
            "timestamp,project,file,psm_count,parsing_time,persist_time,total_time,psm_per_second\n"
        )

        for entry in analyzer.entries:
            f.write(
                f"{entry.timestamp.isoformat()},"
                f"{entry.project_accession},"
                f"{entry.file_name},"
                f"{entry.psm_count},"
                f"{entry.parsing_time:.4f},"
                f"{entry.persist_time:.4f},"
                f"{entry.total_time:.4f},"
                f"{entry.psm_per_second:.2f}\n"
            )

    console.print(f"[green]Exported {len(analyzer.entries)} entries to {output}[/green]")


from collections import defaultdict

import matplotlib.pyplot as plt


@app_cli.command()
def chart(
    log_dir: Path = typer.Argument(
        ...,
        help="Directory containing JSONL log files",
        exists=True,
        file_okay=False,
        dir_okay=True,
    ),
    output: Path = typer.Option(
        Path("psm_rate_per_minute.png"),
        help="Output PNG file for the chart",
    ),
):
    """Create a matplotlib chart of the average PSM write rate per minute."""
    console = Console()

    with console.status("[bold green]Parsing log files..."):
        analyzer = PSMAnalyzer(log_dir)
        analyzer.parse_logs()

    if not analyzer.entries:
        console.print("[red]No PSM import entries found in log files.[/red]")
        return

    # --- Bucket by minute ---
    bucket = defaultdict(list)

    for e in analyzer.entries:
        minute_bucket = e.timestamp.replace(second=0, microsecond=0)
        bucket[minute_bucket].append(e.psm_per_second)

    # Sort by timestamp
    minutes = sorted(bucket.keys())
    avg_rates = [sum(v) / len(v) for v in (bucket[m] for m in minutes)]

    # --- Plot ---
    plt.figure(figsize=(12, 6))
    plt.plot(minutes, avg_rates)
    plt.title("Average PSM Write Rate per Minute")
    plt.xlabel("Time")
    plt.ylabel("PSM/s (Average)")
    plt.tight_layout()
    plt.savefig(output)

    console.print(f"[green]Chart written to {output}[/green]")


if __name__ == "__main__":
    app_cli()
