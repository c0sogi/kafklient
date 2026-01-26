from .base_client import KafkaBaseClient, create_aio_consumer, create_aio_producer
from .listener import KafkaListener
from .rpc import KafkaRPC, KafkaRPCServer

# Aliases for backward compatibility (now using AIO versions)
create_consumer = create_aio_consumer
create_producer = create_aio_producer

__all__ = [
    "KafkaBaseClient",
    "KafkaListener",
    "KafkaRPC",
    "KafkaRPCServer",
    "create_aio_consumer",
    "create_aio_producer",
    "create_consumer",
    "create_producer",
]
