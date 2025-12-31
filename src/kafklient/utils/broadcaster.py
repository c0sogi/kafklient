import asyncio
from dataclasses import dataclass, field
from logging import getLogger
from typing import (
    AsyncIterator,
    Awaitable,
    Callable,
    Generic,
    Literal,
    Optional,
    TypeAlias,
    TypeVar,
)

T = TypeVar("T")
logger = getLogger(__name__)

CallbackPolicy: TypeAlias = Literal[
    "merge",
    "concat",
    "exhaust",
    "switch",
]


class BroadcasterStoppedError(RuntimeError):
    """Raised when waiting for the next item but the broadcaster is stopping/stopped."""


@dataclass
class Callback(Generic[T]):
    name: str
    callback: Callable[[T], Awaitable[None]]
    policy: CallbackPolicy = "merge"
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
    _callback_tasks: set[asyncio.Task[None]] = field(default_factory=set[asyncio.Task[None]], init=False, repr=False)
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
        async with self._cond:
            self._cond.notify_all()
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        tasks = list(self._callback_tasks)
        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._callback_tasks.clear()

        for callback in list(self._callbacks.values()):
            if callback.task is not None and not callback.task.done():
                callback.task.cancel()
                try:
                    await callback.task
                except asyncio.CancelledError:
                    pass
            callback.task = None

    async def wait_next(self, after_version: int) -> T:
        async with self._cond:
            while self._version <= after_version and not self._stopping:
                await self._cond.wait()
            # Only raise on stop if there is still no newer item available.
            if self._version <= after_version and self._stopping:
                raise BroadcasterStoppedError(f"{self.name} broadcaster stopped while waiting for next item")
            assert self._latest_item is not None
            return self._latest_item

    def register_callback(self, callback: Callback[T]) -> None:
        self._callbacks[callback.name] = callback

    def unregister_callback(self, name: str) -> None:
        self._callbacks.pop(name, None)

    async def _run_consumer(self) -> None:
        backoff_s = 0.1
        max_backoff_s = 5.0

        while not self._stopping:
            try:
                # Single shared stream; do not set fresh=True to avoid resetting upstream buffers
                stream = await self.listener()
                async for item in stream:
                    backoff_s = 0.1

                    async with self._cond:
                        self._latest_item = item
                        self._version += 1
                        self._cond.notify_all()

                    # Fire callbacks without blocking the consumer loop
                    for cb in list(self._callbacks.values()):
                        if cb.policy == "exhaust" and cb.task is not None and not cb.task.done():
                            continue
                        if cb.policy == "switch" and cb.task is not None and not cb.task.done():
                            old = cb.task
                            old.cancel()
                            # Defensive: ensure cancelled tasks are tracked and always removed from _callback_tasks
                            # when they finish, even if callback policy overwrites `cb.task`.
                            if old not in self._callback_tasks:
                                self._callback_tasks.add(old)

                                def _discard(
                                    task: asyncio.Task[None], *, _tasks: set[asyncio.Task[None]] = self._callback_tasks
                                ) -> None:
                                    _tasks.discard(task)

                                old.add_done_callback(_discard)
                        prev = cb.task if cb.policy == "concat" else None
                        t = asyncio.create_task(
                            self._safe_call_concat(cb, item, prev)
                            if cb.policy == "concat"
                            else self._safe_call(cb, item),
                            name=f"{self.name}-broadcaster-callback-{cb.name}",
                        )
                        cb.task = t
                        self._callback_tasks.add(t)

                        def _clear(
                            task: asyncio.Task[None],
                            *,
                            _cb: Callback[T] = cb,
                            _tasks: set[asyncio.Task[None]] = self._callback_tasks,
                        ) -> None:
                            _tasks.discard(task)
                            if _cb.task is task:
                                _cb.task = None

                        t.add_done_callback(_clear)

                    if self._stopping:
                        break

                if self._stopping:
                    break

                # Listener stream ended unexpectedly; restart with backoff
                logger.warning("Listener stream ended; restarting consumer loop")
            except asyncio.CancelledError:
                raise
            except Exception:
                # Keep server alive; retry with backoff.
                logger.exception("Error in consumer; restarting consumer loop")

            await asyncio.sleep(backoff_s)
            backoff_s = min(backoff_s * 2.0, max_backoff_s)

    async def _safe_call_concat(
        self,
        cb: Callback[T],
        item: T,
        prev: asyncio.Task[None] | None,
    ) -> None:
        if prev is not None and not prev.done():
            try:
                await prev
            except asyncio.CancelledError:
                # If *this* task is being cancelled (e.g. Broadcaster.stop()), do not swallow it.
                # Otherwise, the awaited previous task was cancelled; treat it as non-fatal and proceed.
                current = asyncio.current_task()
                if current is not None and current.cancelling():
                    raise
            except Exception:
                # Previous callback failures should not block subsequent concat calls.
                pass
        await self._safe_call(cb, item)

    async def _safe_call(self, cb: Callback[T], item: T) -> None:
        try:
            logger.info(f"Calling callback: {cb.name}")
            await cb.callback(item)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Ignore user callback exceptions
            logger.exception(f"Error in callback: {cb.name}")
            return
