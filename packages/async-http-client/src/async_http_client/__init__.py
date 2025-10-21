import logging
import os
from typing import Any

from aiohttp_client_cache import FileBackend, SQLiteBackend
from aiohttp_client_cache.session import CachedSession
from aiohttp_retry import ExponentialRetry, RetryClient

logger = logging.getLogger(__name__)

CLUSTER_CACHE_DIR = "/sc/projects/sci-renard/usi-grabber/.cache/http"
LOCAL_CACHE_FILE_NAME = "aiohttp-cache"


class AsyncHttpClient:
    """
    Async HTTP fetcher with caching, retries, and concurrency limits.
    Uses a shared file system cache on the cluster and else a sqlite db locally.

    Also implements exponential retry
    """

    def __init__(
        self,
        cache_expire: int = 60 * 60 * 24 * 30,  # 30 days
        retry_attempts: int = 3,
        timeout: int = 30,
    ):
        if os.path.isdir(CLUSTER_CACHE_DIR):
            self._is_cache_shared: bool = True
        else:
            self._is_cache_shared = False

        self._session: RetryClient | None = None
        self.retry_attempts: int = retry_attempts
        self.cache_expire: int = cache_expire

    async def __aenter__(self):
        if self._is_cache_shared:
            backend = FileBackend(
                cache_dir=CLUSTER_CACHE_DIR,
                expire_after=self.cache_expire,
            )
        else:
            backend = SQLiteBackend(LOCAL_CACHE_FILE_NAME)
        retry_opts = ExponentialRetry(
            attempts=self.retry_attempts,
        )
        self._session = RetryClient(
            client_session=CachedSession(
                cache=backend,
            ),
            retry_options=retry_opts,
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session:
            await self._session.close()

    async def get(self, url: str, **kwargs: Any):
        """Perform a GET request (cached)."""
        if not self._session:
            raise RuntimeError(
                "Session not initialized — use 'async with' to manage lifecycle."
            )
        async with self._session.get(url, **kwargs) as response:
            return await response.text()

    async def post(self, url: str, data: Any = None, json: Any = None, **kwargs: Any):
        """Perform a POST request (not cached by default)."""
        if not self._session:
            raise RuntimeError(
                "Session not initialized — use 'async with' to manage lifecycle."
            )
        async with self._session.post(url, data=data, json=json, **kwargs) as response:
            return await response.text()
