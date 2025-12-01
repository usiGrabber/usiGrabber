import logging
import os
import re
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp_client_cache import FileBackend, SQLiteBackend
from aiohttp_client_cache.session import CachedSession
from aiohttp_retry import ExponentialRetry, RetryClient
from tqdm import tqdm

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
        verbose: bool = True,
    ):
        if os.path.isdir(CLUSTER_CACHE_DIR):
            self._is_cache_shared: bool = True
        else:
            self._is_cache_shared = False

        self._session: RetryClient | None = None
        self.retry_attempts: int = retry_attempts
        self.cache_expire: int = cache_expire
        self._verbose = verbose

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

    async def get_response(self, url: str, params: dict | None = None, **kwargs: Any):
        """Perform a GET request (cached)."""
        if not self._session:
            raise RuntimeError("Session not initialized — use 'async with' to manage lifecycle.")
        async with self._session.get(url, params=params, **kwargs) as response:
            return response

    async def get(
        self,
        url: str,
        params: dict | None = None,
        parse_json: bool = False,
        valid_response_codes: frozenset[int] = frozenset({200}),
        **kwargs: Any,
    ):
        """
        - Parses json automatically (if content type header is set to application/json)
        - To force json parsing set `parse_json` to True
        """
        import time
        from urllib.parse import urlparse

        start_time = time.time()
        parsed_url = urlparse(url)
        host = parsed_url.hostname or url

        try:
            # Log connection open
            logger.info(
                f"Opening HTTP connection to {host}",
                extra={
                    "event": "connection_open",
                    "protocol": "http",
                    "host": host,
                },
            )

            response = await self.get_response(url, params=params, **kwargs)

            # Check if response came from cache
            is_cached = (
                getattr(response, "from_cache", False)
                or getattr(response, "is_expired", False) is False
            )

            elapsed_time = time.time() - start_time

            if response.status not in valid_response_codes:
                logger.error(
                    f"HTTP request failed with status {response.status}",
                    extra={
                        "event": "http_error",
                        "url": url,
                        "host": host,
                        "status_code": response.status,
                        "response_time": elapsed_time,
                        "is_cached": is_cached,
                    },
                )
                raise ValueError(
                    f"Response: {response.status} from {url}",
                    "not in valid responses ({valid_response_codes}):",
                )

            # Log successful request
            if is_cached:
                logger.info(
                    f"HTTP cache hit for {url}",
                    extra={
                        "event": "http_cache_hit",
                        "url": url,
                        "host": host,
                        "response_time": elapsed_time,
                    },
                )
            else:
                logger.info(
                    f"HTTP request successful to {url}",
                    extra={
                        "event": "http_success",
                        "url": url,
                        "host": host,
                        "status_code": response.status,
                        "response_time": elapsed_time,
                        "is_cached": False,
                    },
                )

            # Log connection close
            logger.info(
                f"Closing HTTP connection to {host}",
                extra={
                    "event": "connection_close",
                    "protocol": "http",
                    "host": host,
                },
            )

            if response.content_type == "application/json":
                return await response.json()
            elif parse_json:
                # Force JSON parsing even if content type is incorrect (e.g., from cache)
                return await response.json(content_type=None)
            else:
                return await response.text()

        except Exception as e:
            elapsed_time = time.time() - start_time

            # Log connection close on error
            logger.info(
                f"Closing HTTP connection to {host} (error)",
                extra={
                    "event": "connection_close",
                    "protocol": "http",
                    "host": host,
                },
            )

            logger.error(
                f"HTTP request failed: {type(e).__name__}",
                exc_info=True,
                extra={
                    "event": "http_failure",
                    "error_type": type(e).__name__,
                    "url": url,
                    "host": host,
                    "response_time": elapsed_time,
                },
            )
            raise

    async def post(self, url: str, data: Any = None, json: Any = None, **kwargs: Any):
        """Perform a POST request (not cached by default)."""
        if not self._session:
            raise RuntimeError("Session not initialized — use 'async with' to manage lifecycle.")
        async with self._session.post(url, data=data, json=json, **kwargs) as response:
            return await response.text()

    async def stream_file(
        self,
        url: str,
        download_file_name: Path | None = None,
        params: dict | None = None,
    ) -> Path:
        """
        Stream a file from URL to disk. Does not cache or retry

        Args:
                url: URL to download from
                download_file_name: Optional path to save the file
                params: Optional query parameters

        Returns:
                Path to the downloaded file
        """
        if not self._session:
            raise RuntimeError("Session not initialized — use 'async with' to manage lifecycle.")

        # Get response - cached or uncached
        async with aiohttp.ClientSession() as session:
            response = await session.get(url, params=params)
            VALID_RESPONSE_CODES = [200]
            if response.status not in VALID_RESPONSE_CODES:
                raise ValueError(
                    f"Response: {response.status} not in okay responses ({VALID_RESPONSE_CODES})"
                )
            total_size = int(response.headers.get("Content-Length", 0))

            # Try to get the filename from the headers
            cd = response.headers.get("Content-Disposition")
            if cd:
                match = re.search(r'filename="?(?P<filename>[^"]+)"?', cd)
                if match:
                    filename = match.group("filename")
                    if not download_file_name:
                        download_file_name = Path(filename)

            if not download_file_name:
                # fallback to something generic
                download_file_name = Path(url.split("/")[-1] or "downloaded_file")

            chunk_size = 4096

            with (
                open(download_file_name, "wb") as f,
                tqdm.wrapattr(f, "write", total=total_size) as fobj,
            ):
                while True:
                    chunk = await response.content.read(chunk_size)
                    if not chunk:
                        break
                    fobj.write(chunk)

            return download_file_name
