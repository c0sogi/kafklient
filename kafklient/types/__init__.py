from typing import (
    Awaitable,
    Callable,
    Generic,
    Type,
    TypedDict,
    TypeVar,
    Union,
)

from .backend import (
    OFFSET_END,
    AdminClient,
    ClusterMetadata,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    NewTopic,
    Producer,
    TopicPartition,
)
from .config import CommonConfig, ConsumerConfig, ProducerConfig

T = TypeVar("T")
T_Co = TypeVar("T_Co", covariant=True)

# Parser callback can be sync or async
ParserCallback = Callable[[Message], Union[T_Co, Awaitable[T_Co]]]

# Correlation extractor callback can be sync or async
CorrelationCallback = Callable[[Message, object], Union[bytes | None, Awaitable[bytes | None]]]


class ParserSpec(TypedDict, Generic[T_Co]):
    """Specify the parser and the range of Kafka input (consume) in one go"""

    topics: list[str]
    type: Type[T_Co]
    parser: ParserCallback[T_Co]


__all__ = [
    "ClusterMetadata",
    "Consumer",
    "CorrelationCallback",
    "Producer",
    "KafkaError",
    "Message",
    "OFFSET_END",
    "TopicPartition",
    "KafkaException",
    "ParserSpec",
    "ConsumerConfig",
    "ProducerConfig",
    "CommonConfig",
    "T_Co",
    "AdminClient",
    "NewTopic",
]
