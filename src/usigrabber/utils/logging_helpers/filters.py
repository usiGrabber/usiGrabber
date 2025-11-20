import logging

from cachetools import LRUCache


class ExponentialBackoffFilter(logging.Filter):
    """
    Logs only on the 1st occurrence and on occurrences that are powers of 2:
    1, 2, 4, 8, 16, ...
    Uses an LRU cache to avoid unbounded memory growth.
    """

    def __init__(self, max_keys=2048):
        """
        max_keys: maximum number of unique messages to track
        """
        super().__init__()
        self._counts = LRUCache(maxsize=max_keys)

    def _make_key(self, record: logging.LogRecord):
        # You can customize: group by (level, message) or exception type, etc.
        # Message is only the raw message (not the formatted final one!)
        # This excludes the timestamp, level, etc

        return (record.levelno, record.getMessage())

    def filter(self, record):
        key = self._make_key(record)
        # Increment occurrence count in LRU cache
        n = self._counts[key] + 1 if key in self._counts else 1
        self._counts[key] = n

        # Log only if count is a power of 2
        # n & (n - 1) == 0 is a fast power-of-2 check
        return (n & (n - 1)) == 0
