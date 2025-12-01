kafklient
=========

Async Kafka client utilities for SDML built on confluent_kafka with thread-based async wrappers.

What's inside
-------------
- `KafkaListener`: subscribe to topics and stream parsed objects.
- `KafkaRPC`: request/response helper using correlation IDs over Kafka.
- Group-managed subscriptions only (subscribe-based). Manual assign is removed.

Requirements
------------
- Python >= 3.12
- Kafka cluster reachable from your app
- confluent-kafka >= 2.6.0

Install
-------
```bash
pip install kafklient
```

Core concepts
-------------
- Group-managed subscribe: Consumers must have a non-empty `group_id` to join a consumer group. Kafka partitions are assigned and rebalanced by the coordinator.
- group_id strategy:
  - Same `group_id` among instances → competing consumers (load-balancing, each record processed once by the group).
  - Different `group_id` among instances → each instance receives the full stream (broadcast-style consumption).
  - Never share `group_id` between logically different roles (e.g., RPC clients vs RPC servers).

ParserSpec
----------
You declare which topics a client parses and how to parse them.

```python
from kafklient import Message, ParserSpec


def parse_json(rec: Message) -> dict[str, object]:
    import json

    return json.loads(rec.value() or b"{}")


spec: ParserSpec[dict[str, object]] = {
    "topics": ["events"],
    "type": dict[str, object],
    "parser": parse_json,
}

```

KafkaListener quickstart
------------------------
```python
import asyncio

from kafklient import KafkaListener


async def main() -> None:
    async with KafkaListener(
        parsers=[
            {
                "topics": ["my-topic"],
                "type": dict[str, object],
                "parser": lambda r: {"topic": r.topic(), "value": (r.value() or b"").decode("utf-8")},
            }
        ],
        consumer_factory={
            "bootstrap.servers": "127.0.0.1:9092",
            "auto.offset.reset": "latest",
        },
    ) as listener:
        stream = await listener.subscribe(dict[str, object])
        async for item in stream:
            print("got:", item)


asyncio.run(main())

```

KafkaRPC quickstart
-------------------
```python
import asyncio

from kafklient import KafkaRPC


async def main() -> None:
    async with KafkaRPC(
        parsers=[{"topics": ["my-topic"], "type": bytes, "parser": lambda r: r.value() or b""}],
        producer_factory={"bootstrap.servers": "127.0.0.1:9092"},
        consumer_factory={
            "bootstrap.servers": "127.0.0.1:9092",
            "auto.offset.reset": "latest",
        },
    ) as rpc:
        res = await rpc.request(
            req_topic="request",
            req_value=b"hello",
            # Optionally direct server to respond to specific topics
            req_headers_reply_to=["reply"],
            res_expect_type=bytes,
        )
        print("response:", res)


asyncio.run(main())

```

RPC server pattern
------------------
Typical layout: servers consume from `request` and produce to reply topics passed in headers.

Guidelines:
- Server instances should share the same `group_id` to load-balance requests.
- Servers must NOT share `group_id` with clients.
- Server reads request, extracts `x-reply-topic` headers, and produces the response to that topic. If multiple reply topics are present, produce to each (or choose policy).

Group_id guidance
-----------------
- Listener
  - Same group_id → scale-out (each record processed once by the group).
  - Different group_id → broadcast (each listener gets all records).
- RPC server (responders)
  - Same group_id among servers → load-balancing for requests.
  - Different group_id among servers → all servers handle each request (usually wrong for RPC).
- RPC client (requesters)
  - Each client should have a unique group_id to avoid competing on replies.
  - Do not reuse server group_id.

Offsets & auto commit
---------------------
- `auto_offset_reset` defaults are set on consumer factories in examples to `latest`.

Correlation IDs
---------------
- `KafkaBaseClient` extracts correlation IDs from headers (e.g. `x-corr-id`) or from the key when present.
- `KafkaRPC.request` can propagate the correlation ID in key and/or a header you choose (default header: `x-corr-id`).

Thread-based async implementation
---------------------------------
This library uses sync `Consumer` and `Producer` from confluent-kafka, wrapped with dedicated thread executors (`DedicatedThreadExecutor`) to provide a non-blocking async API. This approach:
- Avoids blocking the event loop during Kafka operations
- Uses separate dedicated threads for consumer and producer operations
- Provides stable, production-ready Kafka client behavior
- Works with all existing confluent-kafka features and configurations

Auto topic creation
-------------------
Both `KafkaListener` and `KafkaRPC` can automatically create topics if they don't exist. Enable this feature with `auto_create_topics=True`:

```python
async with KafkaListener(
    parsers=[{"topics": ["my-topic"], "type": bytes, "parser": lambda r: r.value() or b""}],
    consumer_factory={"bootstrap.servers": "127.0.0.1:9092"},
    auto_create_topics=True,
    topic_num_partitions=3,      # default: 1
    topic_replication_factor=1,  # default: 1
) as listener:
    # Topics are created before subscribing
    stream = await listener.subscribe(bytes)
    async for item in stream:
        print(item)

```

Options:
- `auto_create_topics: bool = False` - Enable automatic topic creation
- `topic_num_partitions: int = 1` - Number of partitions for auto-created topics
- `topic_replication_factor: int = 1` - Replication factor for auto-created topics

Note: This uses AdminClient internally and requires appropriate broker permissions.

Production notes
----------------
- Always set explicit `group_id` in your provided `consumer_factory`.
- Use dedicated topics for requests and for replies. Avoid sending replies to the request topic.
- Isolate roles with different `group_id`s (clients vs servers).
- Ensure idempotency in servers when necessary.

API reference (selected)
------------------------
- `KafkaListener(parsers: Iterable[ParserSpec[object]], ...)`
  - `subscribe(tp: Type[T], *, queue_maxsize: int = 0, fresh: bool = False) -> TypeStream[T]`
- `KafkaRPC(parsers: Iterable[ParserSpec[object]], ...)`
  - `request(req_topic: str, req_value: bytes, *, req_key: bytes | None = None, req_headers: list[tuple[str, str | bytes]] | None = None, req_headers_reply_to: list[str] | None = None, res_timeout: float = 30.0, res_expect_type: Type[T] | None = None, correlation_id: bytes | None = None, propagate_corr_to: str = "both", correlation_header_key: str = "x-corr-id") -> T`

License
-------
MIT

