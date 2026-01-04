from kafklient.types.config import ConsumerConfig, ProducerConfig

MCP_REPLY_TOPIC_HEADER_KEY = "x-reply-topic"
MCP_SESSION_ID_HEADER_KEY = "x-session-id"

MCP_KAFKA_MAX_MESSAGE_BYTES = 50 * 1024 * 1024  # 50MB
DEFAULT_MCP_CONSUMER_CONFIG: ConsumerConfig = {
    "auto.offset.reset": "latest",
    # Large responses/notifications may also require larger fetch sizes on the consumer side
    # in some deployments; setting them here keeps request consumption robust.
    "fetch.message.max.bytes": MCP_KAFKA_MAX_MESSAGE_BYTES,
    "max.partition.fetch.bytes": MCP_KAFKA_MAX_MESSAGE_BYTES,
}
DEFAULT_MCP_PRODUCER_CONFIG: ProducerConfig = {
    "message.max.bytes": MCP_KAFKA_MAX_MESSAGE_BYTES,
    "compression.type": "zstd",
}
