#!/usr/bin/env python3
"""
Compare performance metrics from two profiling runs.

Usage:
    python scripts/compare_profiles.py baseline_dir optimized_dir [--project PXD000001]
    python scripts/compare_profiles.py baseline optimized
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any


class Colors:
    """ANSI color codes for terminal output."""

    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    END = "\033[0m"


def load_metrics(directory: Path, project_accession: str | None = None) -> dict[str, Any]:
    """
    Load metrics JSON from a profile directory.

    Args:
        directory: Path to profile results directory
        project_accession: Optional project accession (auto-detected if not provided)

    Returns:
        Metrics dictionary

    Raises:
        FileNotFoundError: If metrics file not found
    """
    directory = Path(directory)

    # If project_accession provided, use it directly
    if project_accession:
        metrics_file = directory / f"{project_accession}_metrics.json"
    else:
        # Auto-detect: find any *_metrics.json file
        metrics_files = list(directory.glob("*_metrics.json"))
        if not metrics_files:
            raise FileNotFoundError(f"No metrics JSON found in {directory}")
        if len(metrics_files) > 1:
            raise ValueError(
                f"Multiple metrics files found in {directory}. Please specify --project explicitly."
            )
        metrics_file = metrics_files[0]

    if not metrics_file.exists():
        raise FileNotFoundError(f"Metrics file not found: {metrics_file}")

    with open(metrics_file) as f:
        return json.load(f)


def format_delta(value: float, percent: float, unit: str = "", lower_is_better: bool = True) -> str:
    """
    Format a delta value with color coding.

    Args:
        value: Absolute change
        percent: Percentage change
        unit: Unit string (e.g., 's', 'MB')
        lower_is_better: Whether lower values are better (green) or worse (red)

    Returns:
        Formatted string with color codes
    """
    if abs(percent) < 0.5:
        color = Colors.YELLOW
        symbol = "≈"
    elif (percent < 0 and lower_is_better) or (percent > 0 and not lower_is_better):
        color = Colors.GREEN
        symbol = "↓" if percent < 0 else "↑"
    else:
        color = Colors.RED
        symbol = "↑" if percent > 0 else "↓"

    return f"{color}{symbol} {value:+.2f}{unit} ({percent:+.1f}%){Colors.END}"


def print_header(title: str) -> None:
    """Print a formatted section header."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 70}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{title:^70}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 70}{Colors.END}\n")


def print_metric(
    label: str, baseline: float, optimized: float, unit: str = "", lower_is_better: bool = True
) -> None:
    """Print a comparison of a single metric."""
    delta = optimized - baseline
    percent = (delta / baseline) * 100 if baseline != 0 else 0

    delta_str = format_delta(delta, percent, unit, lower_is_better)

    print(f"{label:.<40} {baseline:.2f}{unit} → {optimized:.2f}{unit}")
    print(f"{'':.<40} {delta_str}")


def compare_configs(baseline_config: dict, optimized_config: dict) -> None:
    """Compare configuration differences between runs."""
    print(f"\n{Colors.BOLD}Configuration Differences:{Colors.END}")

    differences = []
    for key in baseline_config:
        if baseline_config[key] != optimized_config.get(key):
            differences.append((key, baseline_config[key], optimized_config.get(key)))

    if differences:
        for key, baseline_val, optimized_val in differences:
            print(f"  {Colors.YELLOW}⚠{Colors.END}  {key}: {baseline_val} → {optimized_val}")
    else:
        print(f"  {Colors.GREEN}✓{Colors.END} Configurations are identical")


def print_summary(time_pct: float, mem_pct: float | None) -> None:
    """Print overall summary with verdict."""
    print_header("Summary")

    # Time verdict
    if abs(time_pct) < 1:
        time_verdict = f"{Colors.YELLOW}No significant change{Colors.END}"
    elif time_pct < -5:
        time_verdict = f"{Colors.GREEN}{Colors.BOLD}Significant improvement!{Colors.END}"
    elif time_pct < 0:
        time_verdict = f"{Colors.GREEN}Minor improvement{Colors.END}"
    elif time_pct < 5:
        time_verdict = f"{Colors.RED}Minor regression{Colors.END}"
    else:
        time_verdict = f"{Colors.RED}{Colors.BOLD}Significant regression!{Colors.END}"

    print(f"Time:   {time_verdict}")

    # Memory verdict
    if mem_pct is not None:
        if abs(mem_pct) < 1:
            mem_verdict = f"{Colors.YELLOW}No significant change{Colors.END}"
        elif mem_pct < -5:
            mem_verdict = f"{Colors.GREEN}{Colors.BOLD}Significant improvement!{Colors.END}"
        elif mem_pct < 0:
            mem_verdict = f"{Colors.GREEN}Minor improvement{Colors.END}"
        elif mem_pct < 5:
            mem_verdict = f"{Colors.RED}Minor regression{Colors.END}"
        else:
            mem_verdict = f"{Colors.RED}{Colors.BOLD}Significant regression!{Colors.END}"

        print(f"Memory: {mem_verdict}")

    # Overall verdict
    print()
    if time_pct < -5 or (mem_pct is not None and mem_pct < -5):
        print(f"{Colors.GREEN}{Colors.BOLD}✓ Optimization successful!{Colors.END}")
    elif time_pct > 5 or (mem_pct is not None and mem_pct > 5):
        print(f"{Colors.RED}{Colors.BOLD}✗ Performance regression detected{Colors.END}")
    else:
        print(f"{Colors.YELLOW}○ Mixed or minimal impact{Colors.END}")


def main():
    parser = argparse.ArgumentParser(
        description="Compare performance metrics from two profiling runs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare two profile directories
  python scripts/compare_profiles.py baseline optimized

  # Specify project accession explicitly
  python scripts/compare_profiles.py baseline optimized --project PXD000001

  # Compare with JSON output
  python scripts/compare_profiles.py baseline optimized --json > comparison.json
        """,
    )

    parser.add_argument(
        "baseline_dir", type=Path, help="Directory containing baseline profiling results"
    )
    parser.add_argument(
        "optimized_dir", type=Path, help="Directory containing optimized profiling results"
    )
    parser.add_argument(
        "--project", "-p", type=str, help="Project accession (auto-detected if not provided)"
    )
    parser.add_argument("--json", action="store_true", help="Output comparison as JSON")

    args = parser.parse_args()

    try:
        # Load metrics
        baseline = load_metrics(args.baseline_dir, args.project)
        optimized = load_metrics(args.optimized_dir, args.project)

        # Extract values
        baseline_time = baseline["total_time_seconds"]
        optimized_time = optimized["total_time_seconds"]

        baseline_mem = baseline.get("total_memory_mb")
        optimized_mem = optimized.get("total_memory_mb")

        # Calculate changes
        time_delta = optimized_time - baseline_time
        time_pct = (time_delta / baseline_time) * 100

        mem_delta = None
        mem_pct = None
        if baseline_mem is not None and optimized_mem is not None:
            mem_delta = optimized_mem - baseline_mem
            mem_pct = (mem_delta / baseline_mem) * 100

        # JSON output mode
        if args.json:
            result = {
                "baseline": {
                    "time_seconds": baseline_time,
                    "memory_mb": baseline_mem,
                },
                "optimized": {
                    "time_seconds": optimized_time,
                    "memory_mb": optimized_mem,
                },
                "delta": {
                    "time_seconds": time_delta,
                    "time_percent": time_pct,
                    "memory_mb": mem_delta,
                    "memory_percent": mem_pct,
                },
                "verdict": {
                    "time": "improvement"
                    if time_pct < -1
                    else "regression"
                    if time_pct > 1
                    else "neutral",
                    "memory": "improvement"
                    if mem_pct and mem_pct < -1
                    else "regression"
                    if mem_pct and mem_pct > 1
                    else "neutral"
                    if mem_pct is not None
                    else "unknown",
                },
            }
            print(json.dumps(result, indent=2))
            return

        # Human-readable output
        print_header("Performance Comparison")

        # Project info
        project_acc = baseline["config"]["project_accession"]
        print(f"Project: {Colors.BOLD}{project_acc}{Colors.END}")
        print(f"Baseline:  {args.baseline_dir}")
        print(f"Optimized: {args.optimized_dir}")

        # Compare configurations
        compare_configs(baseline["config"], optimized["config"])

        # Performance metrics
        print_header("Performance Metrics")

        print_metric("Execution Time", baseline_time, optimized_time, "s", lower_is_better=True)

        if baseline_mem is not None and optimized_mem is not None:
            print()
            print_metric("Peak Memory", baseline_mem, optimized_mem, " MB", lower_is_better=True)
        else:
            print(f"\n{'Peak Memory':.<40} {Colors.YELLOW}Not available{Colors.END}")

        # Summary
        print_summary(time_pct, mem_pct)

        print()  # Final newline

        # Exit code based on performance
        if time_pct > 10 or (mem_pct is not None and mem_pct > 10):
            sys.exit(1)  # Significant regression

    except FileNotFoundError as e:
        print(f"{Colors.RED}Error: {e}{Colors.END}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"{Colors.RED}Error: {e}{Colors.END}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
