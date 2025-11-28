import asyncio
import uuid
from dataclasses import InitVar, dataclass, field
from typing import Callable, Optional, Type, cast

from .._logging import get_logger
from ..types import T_Co
from ..types.backend import Consumer, Message
from ..utils.task import TypeStream
from .base_client import KafkaBaseClient

logger = get_logger(__name__)


@dataclass
class KafkaListener(KafkaBaseClient):
    """A Kafka listener that subscribes to topics and returns a stream of objects"""

    consumer_factory: InitVar[Callable[[], Consumer]] = lambda: Consumer({
        "bootstrap.servers": "127.0.0.1:9092",
        "group.id": f"listener-{uuid.uuid4().hex}",
        "auto.offset.reset": "latest",
    })
    _subscriptions: dict[Type[object], tuple[asyncio.Queue[object], asyncio.Event]] = field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(
        self,
        corr_from_record: Optional[Callable[[Message, Optional[object]], Optional[bytes]]],
        consumer_factory: Callable[[], Consumer],
    ) -> None:
        super().__post_init__(corr_from_record)
        self._consumer_factory = consumer_factory

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
        if fresh or tp not in self._subscriptions:
            # Replace with a completely new queue/event
            self._subscriptions[tp] = (
                asyncio.Queue(maxsize=queue_maxsize),
                asyncio.Event(),
            )
        q, event = self._subscriptions[tp]
        return TypeStream[T_Co](cast(asyncio.Queue[T_Co], q), event)

    async def _on_record(
        self,
        record: Message,
        parsed_candidates: list[tuple[object, Type[object]]],
        cid: Optional[bytes],
    ) -> None:
        for obj, ot in parsed_candidates:
            q_event = self._subscriptions.get(ot)
            if q_event is None:
                continue
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
        for _q, event in self._subscriptions.values():
            try:
                event.set()
            except Exception:
                pass
        self._subscriptions.clear()
