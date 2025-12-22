import asyncio
import contextlib
from logging import getLogger
from typing import Generic, NamedTuple, Optional, Self, Type

from ..types import T_Co

logger = getLogger(__name__)


class Waiter(NamedTuple, Generic[T_Co]):
    future: asyncio.Future[T_Co]
    expect_type: Optional[Type[T_Co]]


class TypeStream(Generic[T_Co]):
    def __init__(self, q: asyncio.Queue[T_Co], event: asyncio.Event) -> None:
        self._q = q
        self._event = event

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T_Co:
        # If the event is set, stop the iteration
        if self._event.is_set():
            raise StopAsyncIteration

        get_task = asyncio.create_task(self._q.get())
        ev_task = asyncio.create_task(self._event.wait())
        try:
            done, _ = await asyncio.wait(
                {get_task, ev_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            # If the event is completed first (or at the same time), stop the iteration
            if ev_task in done and ev_task.result() is True:
                get_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await get_task
                raise StopAsyncIteration

            # If the item is received first, return it
            ev_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ev_task
            return get_task.result()

        finally:
            # Prevent leaks: clean up remaining tasks
            for t in (get_task, ev_task):
                if not t.done():
                    t.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await t
