"""Dedicated thread executor for thread-unsafe backends like confluent-kafka."""

import asyncio
import queue
import threading
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class DedicatedThreadExecutor:
    """
    Executes all tasks on a single dedicated thread.

    This is essential for thread-unsafe libraries like confluent-kafka
    where Consumer/Producer must be accessed from the same thread.
    """

    def __init__(self, name: str = "kafka-worker") -> None:
        self._name = name
        self._queue: queue.Queue[tuple[Callable[[], Any], asyncio.Future[Any]] | None] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._started = False
        self._loop: asyncio.AbstractEventLoop | None = None

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        """Start the dedicated worker thread."""
        if self._started:
            return
        self._loop = loop
        self._thread = threading.Thread(target=self._worker_loop, name=self._name, daemon=True)
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        """Stop the dedicated worker thread."""
        if not self._started:
            return
        self._queue.put(None)  # Signal to stop
        if self._thread:
            self._thread.join(timeout=5.0)
        self._started = False
        self._thread = None

    def _worker_loop(self) -> None:
        """Main loop running on the dedicated thread."""
        while True:
            item = self._queue.get()
            if item is None:
                break

            func, future = item
            try:
                result = func()
                if self._loop and not future.done():
                    self._loop.call_soon_threadsafe(future.set_result, result)
            except Exception as e:
                if self._loop and not future.done():
                    self._loop.call_soon_threadsafe(future.set_exception, e)

    async def run(self, func: Callable[[], T]) -> T:
        """Run a function on the dedicated thread and await the result."""
        if not self._started or not self._loop:
            raise RuntimeError("Executor not started")

        future: asyncio.Future[T] = self._loop.create_future()
        self._queue.put((func, future))
        return await future

    @property
    def is_running(self) -> bool:
        return self._started
