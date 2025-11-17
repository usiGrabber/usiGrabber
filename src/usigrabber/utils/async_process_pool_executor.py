import asyncio
from concurrent.futures import ProcessPoolExecutor


class ProcessPoolWithBackpressure:
    """
    Async wrapper that provides:
      - a process pool
      - a bounded number of in-flight tasks (backpressure)
      - clean async shutdown
    """

    def __init__(self, max_workers: int | None = None, max_in_flight: int = 1000):
        self.max_workers = max_workers
        self.max_in_flight = max_in_flight
        self._executor = None
        self._sem = asyncio.Semaphore(self.max_in_flight)

    async def __aenter__(self):
        self._executor = ProcessPoolExecutor(max_workers=self.max_workers)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # Shutdown gracefully inside a thread so it doesn't block event loop
        assert self._executor
        await asyncio.to_thread(self._executor.shutdown, wait=True)
        self._executor = None

    async def submit(self, func, *args, **kwargs):
        """
        Submit a CPU-bound task while enforcing backpressure.
        Returns an awaitable future.
        """
        await self._sem.acquire()

        assert self._executor, (
            "Executor must be available!" + "You must use this via an async context manager"
        )
        # schedule the process pool job
        loop = asyncio.get_running_loop()
        future = await loop.run_in_executor(self._executor, func, *args, **kwargs)

        # when finished, free a slot
        def _release(_):
            self._sem.release()

        future.add_done_callback(_release)

        return future
