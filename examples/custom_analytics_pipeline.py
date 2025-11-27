"""
Example of creating custom analytics pipelines for the log aggregator.

This demonstrates how to compose filter, apply, and aggregate operations
to answer specific analytical questions about your data.
"""

from usigrabber.utils.logging_helpers.aggregator import (
    AddErrorFlag,
    AnalyticsPipeline,
    Average,
    Counter,
    ErrorRate,
    EventFilter,
    RunningLogAggregator,
    SuccessfulDownloadsFilter,
)


def create_custom_pipelines():
    """Create custom analytics pipelines for specific metrics."""

    pipelines = []

    # Example 1: Average response time for successful FTP downloads per host
    # Pipeline: filter successful downloads -> calculate average response_time grouped by host
    pipelines.append(
        AnalyticsPipeline(
            name="avg_response_time_per_host",
            filters=[SuccessfulDownloadsFilter()],
            apply_ops=[],  # No transformations needed
            aggregate_op=Average("response_time", "host"),
        )
    )

    # Example 2: Count total downloads (success + failure) per host
    # Pipeline: filter download events -> count by host
    pipelines.append(
        AnalyticsPipeline(
            name="total_downloads_per_host",
            filters=[
                # Could add EventFilter("download_success") here to filter,
                # but we want all download events
            ],
            apply_ops=[],
            aggregate_op=Counter("host"),
        )
    )

    # Example 3: Error rate per host
    # Pipeline: add error flag -> calculate error rate grouped by host
    pipelines.append(
        AnalyticsPipeline(
            name="error_rate_per_host",
            filters=[],  # No filtering - we want all events
            apply_ops=[AddErrorFlag()],  # Transform: add is_error field
            aggregate_op=ErrorRate("host"),
        )
    )

    # Example 4: Count errors by error type
    # Pipeline: filter failures -> count by error_type
    pipelines.append(
        AnalyticsPipeline(
            name="errors_by_type",
            filters=[EventFilter("download_failure")],
            apply_ops=[],
            aggregate_op=Counter("error_type"),
        )
    )

    return pipelines


def main():
    """Example of using the aggregator with custom pipelines."""
    import time

    # Initialize aggregator
    aggregator = RunningLogAggregator(log_dir="logs")

    # Register custom pipelines
    for pipeline in create_custom_pipelines():
        aggregator.register_pipeline(pipeline)
        print(f"Registered pipeline: {pipeline.name}")

    # Simulate continuous monitoring
    print("\nMonitoring logs (Ctrl+C to stop)...")
    try:
        while True:
            # Update metrics from log files
            aggregator.update()

            # Get results from specific pipeline
            avg_response_times = aggregator.get_pipeline_results("avg_response_time_per_host")

            print("\n" + "=" * 60)
            print("Average Response Times by Host:")
            print("=" * 60)
            for host, data in avg_response_times.items():
                print(f"  {host}: {data['_average']:.2f}s (n={data['_average_n']})")

            # Get all metrics
            all_metrics = aggregator.get_all_metrics()
            print(f"\nTotal pipelines: {len(all_metrics)}")

            # Wait before next update
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopped monitoring.")


if __name__ == "__main__":
    main()
