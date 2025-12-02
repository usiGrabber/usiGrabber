"""
Demo script showing resource monitoring in action.

Run with:
    RESOURCE_MONITORING_INTERVAL=5 python -m usigrabber.utils.logging_helpers.resource_demo

This will log resource usage every 5 seconds (or whatever interval you set).
"""

import logging
import time

from usigrabber.utils.setup import system_setup

# Setup logging with resource monitoring
system_setup(is_main_process=True, logger_name="usigrabber")

logger = logging.getLogger("usigrabber.demo")


def simulate_work():
    """Simulate some work that consumes resources."""
    logger.info("Starting simulated work")

    # Allocate some memory
    data = []
    for i in range(5):
        # Allocate ~10MB
        chunk = [0] * (10 * 1024 * 1024 // 8)
        data.append(chunk)
        logger.info(f"Allocated chunk {i + 1}, total ~{(i + 1) * 10}MB")
        time.sleep(2)

    # Do some CPU work
    logger.info("Starting CPU-intensive work")
    for i in range(3):
        result = sum(range(10_000_000))
        logger.info(f"CPU work iteration {i + 1}, result: {result}")
        time.sleep(2)

    logger.info("Work completed")
    return data


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Resource Monitoring Demo")
    logger.info("=" * 60)
    logger.info("Watch for resource usage being logged every 60 seconds")
    logger.info("(or at your configured RESOURCE_MONITORING_INTERVAL)")
    logger.info("")

    simulate_work()

    logger.info("")
    logger.info("Demo complete. Check your log files for resource usage data!")
