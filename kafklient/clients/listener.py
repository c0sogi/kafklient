import asyncio
from dataclasses import dataclass, field
from typing import Optional, Type, cast

from .._logging import get_logger
from ..types import T_Co
from ..types.backend import Message
from ..utils.task import TypeStream
from .base_client import KafkaBaseClient

logger = get_logger(__name__)


@dataclass
class KafkaListener(KafkaBaseClient):
    """A Kafka listener that subscribes to topics and returns a stream of objects"""

    subscriptions: dict[Type[object], tuple[asyncio.Queue[object], asyncio.Event]] = field(
        default_factory=dict[Type[object], tuple[asyncio.Queue[object], asyncio.Event]], init=False, repr=False
    )

    async def subscribe(
        self,
        tp: Type[T_Co],
        *,
        queue_maxsize: int = 0,
        fresh: bool = False,
    ) -> TypeStream[T_Co]:
        if self._closed:
            await self.start()
        await self.consumer
        if fresh or tp not in self.subscriptions:
            # Replace with a completely new queue/event
            self.subscriptions[tp] = (
                asyncio.Queue(maxsize=queue_maxsize),
                asyncio.Event(),
            )
        q, event = self.subscriptions[tp]
        return TypeStream[T_Co](cast(asyncio.Queue[T_Co], q), event)

    async def _on_record(self, record: Message, parsed: tuple[object, Type[object]], cid: Optional[bytes]) -> None:
        obj, ot = parsed
        q_event = self.subscriptions.get(ot)
        if q_event is None:
            return
        q, _event = q_event
        try:
            q.put_nowait(obj)
        except asyncio.QueueFull:
            try:
                q.get_nowait()
                q.put_nowait(obj)
            except Exception:
                pass

    async def _on_stop_cleanup(self) -> None:
        for _q, event in self.subscriptions.values():
            try:
                event.set()
            except Exception:
                pass
        self.subscriptions.clear()
