# async-http-client

A small async HTTP client wrapper built on top of [`aiohttp`](https://docs.aiohttp.org/), [`aiohttp-client-cache`](https://aiohttp-client-cache.readthedocs.io/), and [`aiohttp-retry`](https://github.com/inyutin/aiohttp_retry).

## What it does

`AsyncHttpClient` provides a single async context-manager class that combines three concerns:

| Feature | Detail |
|---|---|
| **Caching** | Responses are cached for 30 days by default. On the HPI cluster (where `/sc/projects/sci-renard/usi-grabber/.cache/http` exists) a shared filesystem `FileBackend` is used so all processes share the cache. Locally it falls back to a per-directory SQLite database (`aiohttp-cache.sqlite`). |
| **Retries** | Failed requests are automatically retried up to 3 times with exponential back-off via `aiohttp-retry`. |
| **File streaming** | Large files can be streamed directly to disk with a `tqdm` progress bar. The filename is resolved from the `Content-Disposition` header, the URL path, or a caller-supplied path. |

## Usage

```python
import asyncio
from async_http_client import AsyncHttpClient

async def main():
    async with AsyncHttpClient() as client:

        # JSON — auto-parsed when the server returns application/json,
        # or forced with parse_json=True
        data = await client.get("https://api.example.com/items/1")

        # Stream a large file to disk
        path = await client.stream_file(
            "https://files.example.com/large.zip",
            download_file_name=Path("large.zip"),
        )

asyncio.run(main())
```

### Constructor options

| Parameter | Default | Description |
|---|---|---|
| `cache_expire` | `2592000` (30 days) | Cache TTL in seconds |
| `retry_attempts` | `3` | Number of retry attempts on failure |
| `timeout` | `30` | Request timeout in seconds |
| `verbose` | `True` | Show `tqdm` progress bar during file streaming |
