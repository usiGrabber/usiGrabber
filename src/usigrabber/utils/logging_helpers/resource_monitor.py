import logging
import threading
import time

try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


class ResourceMonitor:
    """
    Background thread that periodically logs memory and CPU usage.

    This runs independently of the logging system, avoiding overhead on each log call.

    Usage:
        monitor = ResourceMonitor(interval_seconds=60, logger_name="usigrabber")
        monitor.start()
        # ... your application runs ...
        monitor.stop()  # Call this on shutdown
    """

    def __init__(self, interval_seconds: float = 60.0, logger_name: str = "usigrabber"):
        """
        Args:
            interval_seconds: How often to log resource usage (in seconds)
            logger_name: Logger name to use for resource logs
        """
        self.interval_seconds = interval_seconds
        self.logger = logging.getLogger(f"{logger_name}.resources")
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._process = psutil.Process() if PSUTIL_AVAILABLE else None  # pyright: ignore[reportPossiblyUnboundVariable]

        if not PSUTIL_AVAILABLE:
            logging.warning(
                "psutil not available. Install with 'pip install psutil' to enable resource monitoring."
            )

    def _get_resource_info(self) -> dict:
        """Get current resource usage information."""
        if not self._process:
            return {}

        try:
            # Get memory info
            mem_info = self._process.memory_info()
            memory_mb = mem_info.rss / (1024 * 1024)  # Convert to MB
            memory_percent = self._process.memory_percent()

            # Get CPU percent (averaged over interval)
            cpu_percent = self._process.cpu_percent(interval=1.0)

            return {
                "memory_mb": round(memory_mb, 2),
                "memory_percent": round(memory_percent, 2),
                "cpu_percent": round(cpu_percent, 2),
            }
        except Exception as e:
            self.logger.debug(f"Error getting resource info: {e}")
            return {}

    def _monitor_loop(self):
        """Background thread loop that periodically logs resource usage."""
        # Wait a bit before first measurement to let CPU percent initialize
        time.sleep(1.0)

        while not self._stop_event.is_set():
            resource_info = self._get_resource_info()

            if resource_info:
                self.logger.info(
                    f"Memory: {resource_info['memory_mb']:.1f}MB "
                    f"({resource_info['memory_percent']:.1f}%), "
                    f"CPU: {resource_info['cpu_percent']:.1f}%",
                    extra=resource_info,
                )

            # Wait for the interval or until stop is signaled
            self._stop_event.wait(self.interval_seconds)

    def start(self):
        """Start the resource monitoring thread."""
        if not self._process:
            self.logger.info("Resource monitoring disabled (psutil not available)")
            return

        if self._thread is not None and self._thread.is_alive():
            self.logger.warning("Resource monitor already running")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._monitor_loop, daemon=True, name="ResourceMonitor"
        )
        self._thread.start()
        self.logger.info(f"Resource monitoring started (interval: {self.interval_seconds}s)")

    def stop(self, timeout: float = 5.0):
        """
        Stop the resource monitoring thread.

        Args:
            timeout: Maximum time to wait for thread to stop (in seconds)
        """
        if self._thread is None or not self._thread.is_alive():
            return

        self._stop_event.set()
        self._thread.join(timeout=timeout)

        if self._thread.is_alive():
            self.logger.warning("Resource monitor thread did not stop gracefully")
        else:
            self.logger.info("Resource monitoring stopped")


# Global instance for easy access
_global_monitor: ResourceMonitor | None = None


def start_resource_monitoring(interval_seconds: float = 60.0, logger_name: str = "usigrabber"):
    """
    Start global resource monitoring.

    This is a convenience function that creates and starts a global monitor instance.
    """
    global _global_monitor

    if _global_monitor is not None:
        logging.warning("Resource monitoring already started")
        return

    _global_monitor = ResourceMonitor(interval_seconds=interval_seconds, logger_name=logger_name)
    _global_monitor.start()


def stop_resource_monitoring():
    """Stop the global resource monitoring."""
    global _global_monitor

    if _global_monitor is not None:
        _global_monitor.stop()
        _global_monitor = None
