import asyncio
from dataclasses import dataclass, field
from typing import AsyncIterator, Awaitable, Callable, Generic, Optional, Self

from .._logging import get_logger
from ..types import T

logger = get_logger(__name__)


@dataclass
class Callback(Generic[T]):
    name: str
    callback: Callable[[T, Self], Awaitable[None]]
    task: Optional[asyncio.Task[None]] = field(default=None, init=False, repr=False)


@dataclass
class Broadcaster(Generic[T]):
    name: str
    listener: Callable[[], Awaitable[AsyncIterator[T]]]

    _latest_item: Optional[T] = field(default=None, init=False, repr=False)
    _version: int = field(default=0, init=False, repr=False)
    _cond: asyncio.Condition = field(default_factory=asyncio.Condition, init=False, repr=False)
    _task: Optional[asyncio.Task[None]] = field(default=None, init=False, repr=False)
    _callbacks: dict[str, Callback[T]] = field(default_factory=dict[str, Callback[T]], init=False, repr=False)
    _stopping: bool = field(default=False, init=False, repr=False)

    @property
    def current_version(self) -> int:
        return self._version

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run_consumer(), name=f"{self.name}-broadcaster-consumer")

    async def stop(self) -> None:
        self._stopping = True
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def wait_next(self, after_version: int) -> T:
        async with self._cond:
            while self._version <= after_version:
                await self._cond.wait()
            assert self._latest_item is not None
            return self._latest_item

    def register_callback(self, callback: Callback[T]) -> None:
        self._callbacks[callback.name] = callback

    def unregister_callback(self, name: str) -> None:
        self._callbacks.pop(name, None)

    async def _run_consumer(self) -> None:
        try:
            # Single shared stream; do not set fresh=True to avoid resetting upstream buffers
            async for item in await self.listener():
                async with self._cond:
                    self._latest_item = item
                    self._version += 1
                    self._cond.notify_all()

                # Fire callbacks without blocking the consumer loop
                for cb in list(self._callbacks.values()):
                    asyncio.create_task(self._safe_call(cb, item))
                if self._stopping:
                    break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # Swallow unexpected errors to keep server alive; backoff handled by upstream listener
            logger.error(f"Error in consumer: {e}")
            await asyncio.sleep(0.1)

    async def _safe_call(self, cb: Callback[T], item: T) -> None:
        try:
            logger.info(f"Calling callback: {cb.name}")
            await cb.callback(item, cb)
        except Exception:
            # Ignore user callback exceptions
            logger.exception(f"Error in callback: {cb.name}")
            return
