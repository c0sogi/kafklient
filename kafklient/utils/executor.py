"""Dedicated thread executor for thread-unsafe backends like confluent-kafka."""

import asyncio
import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar

T = TypeVar("T")


@dataclass
class DedicatedThreadExecutor:
    """
    Executes all tasks on a single dedicated thread.

    This is essential for thread-unsafe libraries like confluent-kafka
    where Consumer/Producer must be accessed from the same thread.
    """

    name: str = "kafka-worker"

    _queue: queue.Queue[tuple[Callable[[], Any], asyncio.Future[Any]] | None] = field(
        default_factory=queue.Queue, init=False, repr=False
    )
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _loop: asyncio.AbstractEventLoop | None = field(default=None, init=False, repr=False)

    def start(self, loop: asyncio.AbstractEventLoop, *, name: str | None = None) -> None:
        """Start the dedicated worker thread."""
        if self.is_running:
            return
        self._loop = loop
        self._thread = threading.Thread(target=self._worker_loop, name=name, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the dedicated worker thread."""
        if not self.is_running:
            return
        self._queue.put(None)  # Signal to stop
        if self._thread:
            self._thread.join(timeout=5.0)
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
        if not self.is_running or not self._loop:
            raise RuntimeError("Executor not started")

        future: asyncio.Future[T] = self._loop.create_future()
        self._queue.put((func, future))
        return await future

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()
