from typing import (
    Callable,
    Generic,
    Type,
    TypedDict,
    TypeVar,
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


class ParserSpec(TypedDict, Generic[T_Co]):
    """Specify the parser and the range of Kafka input (consume) in one go"""

    topics: list[str]
    type: Type[T_Co]
    parser: Callable[[Message], T_Co]


__all__ = [
    "ClusterMetadata",
    "Consumer",
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
