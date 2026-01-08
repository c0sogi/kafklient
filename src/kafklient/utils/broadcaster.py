import asyncio
from collections.abc import ItemsView, Iterator, KeysView, ValuesView
from dataclasses import dataclass, field
from logging import getLogger
from types import TracebackType
from typing import (
    AsyncIterator,
    Awaitable,
    Callable,
    Generic,
    Literal,
    Optional,
    Self,
    TypeAlias,
    TypeVar,
    cast,
    overload,
)

T = TypeVar("T")
logger = getLogger(__name__)

CallbackPolicy: TypeAlias = Literal[
    "merge",
    "concat",
    "exhaust",
    "switch",
]

type CallbackFn[T] = Callable[[T], Awaitable[None]]


class BroadcasterStoppedError(RuntimeError):
    """Raised when waiting for the next item but the broadcaster is stopping/stopped."""


@dataclass
class Callback(Generic[T]):
    name: str
    callback: CallbackFn[T]
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
    _callback_order: list[str] = field(default_factory=list[str], init=False, repr=False)
    _callback_tasks: set[asyncio.Task[None]] = field(default_factory=set[asyncio.Task[None]], init=False, repr=False)
    _stopping: bool = field(default=False, init=False, repr=False)

    @property
    def current_version(self) -> int:
        return self._version

    # --- dict-like callback accessors (name -> Callback) ---

    def __len__(self) -> int:
        return len(self._callbacks)

    def __iter__(self) -> Iterator[str]:
        return iter(self._callbacks)

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and name in self._callbacks

    def keys(self) -> KeysView[str]:
        return self._callbacks.keys()

    def values(self) -> ValuesView[Callback[T]]:
        return self._callbacks.values()

    def items(self) -> ItemsView[str, Callback[T]]:
        return self._callbacks.items()

    def get(self, name: str, default: Callback[T] | None = None) -> Callback[T] | None:
        return self._callbacks.get(name, default)

    def pop(self, name: str) -> Callback[T]:
        cb = self._callbacks.get(name)
        if cb is None:
            raise KeyError(name)
        self.unregister_callback(name)
        return cb

    def __getitem__(self, name: str) -> Callback[T]:
        return self._callbacks[name]

    @overload
    def __setitem__(self, name: str, value: Callback[T]) -> None: ...

    @overload
    def __setitem__(self, name: str, value: CallbackFn[T]) -> None: ...

    @overload
    def __setitem__(self, name: str, value: tuple[CallbackFn[T], CallbackPolicy]) -> None: ...

    def __setitem__(self, name: str, value: Callback[T] | CallbackFn[T] | tuple[CallbackFn[T], CallbackPolicy]) -> None:
        """
        Dict-like registration:

        - ``b["cb"] = callback_fn`` registers a callback with default policy ("merge")
        - ``b["cb"] = (callback_fn, "switch")`` registers with explicit policy
        - ``b["cb"] = Callback(...)`` registers (name is normalized to the key)
        """
        cb: Callback[T]
        if isinstance(value, tuple):
            fn, policy = cast(tuple[CallbackFn[T], CallbackPolicy], value)
            cb = Callback(name=name, callback=fn, policy=policy)
        elif isinstance(value, Callback):
            v = cast(Callback[T], value)
            cb = v if v.name == name else Callback(name=name, callback=v.callback, policy=v.policy)
        else:
            cb = Callback(name=name, callback=value)
        self.register_callback(cb)

    def __delitem__(self, name: str) -> None:
        if name not in self._callbacks:
            raise KeyError(name)
        self.unregister_callback(name)

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

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

    async def wait_next(self, after_version: int | None = None) -> T:
        """
        Wait until a newer item is available.

        If ``after_version`` is omitted, it is treated as the broadcaster's current version at call time,
        i.e. this waits for the *next* publish.
        """
        async with self._cond:
            target_version = self._version if after_version is None else after_version
            while self._version <= target_version and not self._stopping:
                await self._cond.wait()
            # Only raise on stop if there is still no newer item available.
            if self._version <= target_version and self._stopping:
                raise BroadcasterStoppedError(f"{self.name} broadcaster stopped while waiting for next item")
            latest = self._latest_item
            if latest is None:
                # Defensive: should only be possible if stopped before any publish.
                raise BroadcasterStoppedError(f"{self.name} broadcaster stopped while waiting for next item")
            return latest

    def register_callback(
        self,
        callback: Callback[T],
        *,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> None:
        """
        Register a callback with optional ordering.

        Args:
            callback: The callback to register.
            before: Place this callback before the named callback. Mutually exclusive with ``after``.
            after: Place this callback after the named callback. Mutually exclusive with ``before``.

        Raises:
            ValueError: If both ``before`` and ``after`` are specified, or if the reference callback doesn't exist.
        """
        if before is not None and after is not None:
            raise ValueError("Cannot specify both 'before' and 'after'")
        if before is not None and before == callback.name:
            raise ValueError("Cannot register callback with 'before' pointing to itself")
        if after is not None and after == callback.name:
            raise ValueError("Cannot register callback with 'after' pointing to itself")

        # Validate before/after references BEFORE modifying _callback_order
        if before is not None:
            if before not in self._callbacks:
                raise ValueError(f"Callback '{before}' not found")
            if before not in self._callback_order:
                raise ValueError(f"Callback '{before}' not found in callback order")
        elif after is not None:
            if after not in self._callbacks:
                raise ValueError(f"Callback '{after}' not found")
            if after not in self._callback_order:
                raise ValueError(f"Callback '{after}' not found in callback order")

        if callback.name in self._callbacks:
            # Update existing callback, preserve order unless explicitly reordered
            self._callbacks[callback.name] = callback
            if before is None and after is None:
                return
            # Remove from current position (validation already passed)
            if callback.name in self._callback_order:
                self._callback_order.remove(callback.name)
        else:
            self._callbacks[callback.name] = callback

        if before is not None:
            idx = self._callback_order.index(before)
            self._callback_order.insert(idx, callback.name)
        elif after is not None:
            idx = self._callback_order.index(after)
            self._callback_order.insert(idx + 1, callback.name)
        else:
            # Default: append to end
            if callback.name not in self._callback_order:
                self._callback_order.append(callback.name)

    def unregister_callback(self, name: str) -> bool:
        """Unregister a callback by name."""
        removed = self._callbacks.pop(name, None) is not None
        if name in self._callback_order:
            self._callback_order.remove(name)
        return removed

    def reorder_callback(
        self,
        name: str,
        *,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> None:
        """
        Reorder an existing callback.

        Args:
            name: Name of the callback to reorder.
            before: Place this callback before the named callback. Mutually exclusive with ``after``.
            after: Place this callback after the named callback. Mutually exclusive with ``before``.

        Raises:
            ValueError: If the callback doesn't exist, both ``before`` and ``after`` are specified,
                       or the reference callback doesn't exist.
        """
        if name not in self._callbacks:
            raise ValueError(f"Callback '{name}' not found")
        if before is not None and after is not None:
            raise ValueError("Cannot specify both 'before' and 'after'")
        if before is not None and before == name:
            raise ValueError("Cannot reorder callback with 'before' pointing to itself")
        if after is not None and after == name:
            raise ValueError("Cannot reorder callback with 'after' pointing to itself")

        # Validate before/after references BEFORE modifying _callback_order
        if before is not None:
            if before not in self._callbacks:
                raise ValueError(f"Callback '{before}' not found")
            if before not in self._callback_order:
                raise ValueError(f"Callback '{before}' not found in callback order")
        elif after is not None:
            if after not in self._callbacks:
                raise ValueError(f"Callback '{after}' not found")
            if after not in self._callback_order:
                raise ValueError(f"Callback '{after}' not found in callback order")

        if name not in self._callback_order:
            # Callback exists but not in order list, add it (validation already passed)
            if before is not None:
                idx = self._callback_order.index(before)
                self._callback_order.insert(idx, name)
            elif after is not None:
                idx = self._callback_order.index(after)
                self._callback_order.insert(idx + 1, name)
            else:
                self._callback_order.append(name)
            return

        # Remove from current position (validation already passed)
        self._callback_order.remove(name)

        if before is not None:
            idx = self._callback_order.index(before)
            self._callback_order.insert(idx, name)
        elif after is not None:
            idx = self._callback_order.index(after)
            self._callback_order.insert(idx + 1, name)
        else:
            # Default: append to end
            self._callback_order.append(name)

    def get_callback_order(self) -> list[str]:
        """
        Get the current execution order of callbacks.

        Returns:
            List of callback names in execution order.
        """
        return self._callback_order.copy()

    def set_callback_order(self, order: list[str]) -> None:
        """
        Set the execution order of callbacks in bulk.

        Args:
            order: List of callback names in the desired execution order.
                  Must contain all registered callbacks, but can omit some callbacks
                  (omitted callbacks will be appended at the end in their current order).

        Raises:
            ValueError: If any callback name in the order doesn't exist or if duplicates are provided.
        """
        # Validate that the order list doesn't have duplicates
        if len(order) != len(set(order)):
            raise ValueError("Callback order contains duplicate names")

        # Validate that all specified callbacks exist
        for name in order:
            if name not in self._callbacks:
                raise ValueError(f"Callback '{name}' not found")

        # Get callbacks not in the provided order (to append at the end)
        existing_callbacks = set(self._callbacks.keys())
        specified_callbacks = set(order)
        omitted_callbacks = existing_callbacks - specified_callbacks

        # Build the new order: specified order + omitted callbacks in their current order
        new_order = list(order)
        if omitted_callbacks:
            # Preserve current order for omitted callbacks
            current_order = list(self._callback_order) if self._callback_order else list(self._callbacks.keys())
            for name in current_order:
                if name in omitted_callbacks and name not in new_order:
                    new_order.append(name)

        self._callback_order = new_order

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
                    # Use _callback_order to maintain execution order
                    callback_names = (
                        list(self._callback_order) if self._callback_order else list(self._callbacks.keys())
                    )
                    for cb_name in callback_names:
                        if cb_name not in self._callbacks:
                            continue
                        cb = self._callbacks[cb_name]
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
