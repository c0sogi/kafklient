kafklient
=========

Async Kafka utilities built on `confluent-kafka` (librdkafka).

This library wraps the synchronous `Consumer`/`Producer` with dedicated thread executors so Kafka operations do not block
the event loop, and provides typed streams and RPC utilities via `Parser[T]`.

## What's inside

- **`KafkaListener`**: Subscribe to topics and stream parsed objects as `TypeStream[T]`.
- **`KafkaRPC`**: Send requests and await responses matched by correlation id.
- **`KafkaRPCServer`**: Consume request topics and produce responses to reply topics specified in headers.
- **MCP over Kafka (optional)**: Run MCP (JSON-RPC) over Kafka topics + stdio bridges via CLI (`kafklient mcp-client` / `kafklient mcp-server`).

> This library supports **consumer-group subscribe mode only**. (Manual assign is intentionally not supported.)

## Requirements

- Python **>= 3.12**
- A reachable Kafka broker
- `confluent-kafka` **>= 2.12.0**

## Install

```bash
pip install kafklient
```

Optional extras:

```bash
# MCP server/bridge support
pip install "kafklient[mcp]"

# Dev tools (pyright/ruff)
pip install "kafklient[all]"
```

## Local Kafka for development (optional)

This repository includes a single-node Kafka `docker-compose.yml` for local testing.

```bash
docker compose up -d
```

Default bootstrap server: `127.0.0.1:9092`.

## Core concepts

### Consumer group (`group.id`)

- **Same `group.id`**: competing consumers / load balancing (each record is delivered to one member in the group).
- **Different `group.id`**: each instance receives the full stream (broadcast-style consumption).
- **Important**: do not share a `group.id` between different roles (e.g. RPC clients vs RPC servers).

### Start-from-latest behavior (`seek_to_end_on_assign`)

With the default `seek_to_end_on_assign=True`, the consumer seeks to the end when partitions are assigned.
This focuses processing on messages produced after the client becomes ready and reduces accidental reprocessing of old data.

To read from older offsets:

- `seek_to_end_on_assign=False`
- optionally set `consumer_config["auto.offset.reset"] = "earliest"`

### `Parser[T]`

`Parser[T]` declares which topics to parse and what type to parse into.

- **Recommended**: provide a `factory` for JSON/custom binary payloads.
- `factory` can be sync or async; the input is a `Message`.

## Examples

### KafkaListener: consume as a typed stream

This example parses JSON into a `Hello` dataclass and consumes a `TypeStream[Hello]`.

```python
import asyncio
import json
from dataclasses import dataclass

from kafklient import ConsumerConfig, KafkaListener, Message, Parser, ProducerConfig


@dataclass(frozen=True, slots=True)
class Hello:
    message: str
    count: int


def parse_hello(rec: Message) -> Hello:
    raw = rec.value() or b"{}"
    data = json.loads(raw.decode("utf-8"))
    return Hello(
        message=str(data.get("message", "")),
        count=int(data.get("count", 0)),
    )


async def main() -> None:
    topic = "hello-events"

    consumer_config: ConsumerConfig = {
        "bootstrap.servers": "127.0.0.1:9092",
        "group.id": "hello-listener",
        "auto.offset.reset": "latest",
    }
    producer_config: ProducerConfig = {"bootstrap.servers": "127.0.0.1:9092"}

    async with KafkaListener(
        parsers=[Parser[Hello](topics=[topic], factory=parse_hello)],
        consumer_config=consumer_config,
        producer_config=producer_config,
        auto_create_topics=True,
    ) as listener:
        stream = await listener.subscribe(Hello)

        # Demo only: produce and consume in the same process
        await listener.produce(
            topic,
            json.dumps({"message": "hi", "count": 1}).encode("utf-8"),
            flush=True,
        )

        async def receive_one() -> Hello:
            async for item in stream:
                return item
            raise RuntimeError("stream stopped before receiving a message")

        msg = await asyncio.wait_for(receive_one(), timeout=5.0)
        print(msg)


if __name__ == "__main__":
    asyncio.run(main())
```

### KafkaRPC + KafkaRPCServer: request/response

RPC follows these rules:

- **Request topic**: consumed by `KafkaRPCServer` (share the same `group.id` across servers to load-balance).
- **Reply topic**: consumed by `KafkaRPC` (clients typically should use a unique `group.id`).
- **Reply routing**: passed via one or more `x-reply-topic` headers on the request message.
- **Correlation matching**: by default uses the message key (or the `x-corr-id` header).

```python
import asyncio
from dataclasses import dataclass

from kafklient import ConsumerConfig, KafkaRPC, KafkaRPCServer, Message, Parser, ProducerConfig


@dataclass(frozen=True, slots=True)
class EchoRequest:
    data: bytes


def parse_echo_request(msg: Message) -> EchoRequest:
    return EchoRequest(data=msg.value() or b"")


def parse_bytes(msg: Message) -> bytes:
    return msg.value() or b""


async def run_server(*, ready: asyncio.Event, stop: asyncio.Event) -> None:
    request_topic = "rpc-requests"

    server_consumer_config: ConsumerConfig = {
        "bootstrap.servers": "127.0.0.1:9092",
        "group.id": "rpc-server",
        "auto.offset.reset": "latest",
    }
    server_producer_config: ProducerConfig = {"bootstrap.servers": "127.0.0.1:9092"}

    server = KafkaRPCServer(
        parsers=[Parser[EchoRequest](topics=[request_topic], factory=parse_echo_request)],
        consumer_config=server_consumer_config,
        producer_config=server_producer_config,
        auto_create_topics=True,
    )

    @server.handler(EchoRequest)
    async def echo(req: EchoRequest, message: Message) -> bytes:  # pyright: ignore[reportUnusedFunction]
        return req.data

    await server.start()
    ready.set()
    try:
        await stop.wait()
    finally:
        await server.stop()


async def main() -> None:
    request_topic = "rpc-requests"
    reply_topic = "rpc-replies"

    server_ready = asyncio.Event()
    server_stop = asyncio.Event()
    server_task = asyncio.create_task(run_server(ready=server_ready, stop=server_stop))

    rpc_consumer_config: ConsumerConfig = {
        "bootstrap.servers": "127.0.0.1:9092",
        "group.id": "rpc-client-1",
        "auto.offset.reset": "latest",
    }
    rpc_producer_config: ProducerConfig = {"bootstrap.servers": "127.0.0.1:9092"}

    rpc = KafkaRPC(
        parsers=[Parser[bytes](topics=[reply_topic], factory=parse_bytes)],
        consumer_config=rpc_consumer_config,
        producer_config=rpc_producer_config,
        auto_create_topics=True,
    )

    try:
        await server_ready.wait()
        await rpc.start()

        res = await rpc.request(
            req_topic=request_topic,
            req_value=b"ping",
            req_headers_reply_to=[reply_topic],
            res_timeout=5.0,
            res_expect_type=bytes,
        )
        print(res)
    finally:
        await rpc.stop()
        server_stop.set()
        await server_task


if __name__ == "__main__":
    asyncio.run(main())
```

## Automatic topic creation

All clients (`KafkaListener`/`KafkaRPC`/`KafkaRPCServer`) can create topics in one of two ways:

- `auto_create_topics=True`: create topics referenced by `parsers` on start (before subscribing).
- Manual call: `await client.create_topics("a", "b", ...)`

Options:

- `topic_num_partitions` (default: 1)
- `topic_replication_factor` (default: 1)

> Internally this uses `AdminClient` and requires broker permissions.

## MCP over Kafka (optional)

You can run MCP (JSON-RPC) over Kafka topics and connect any stdio-based MCP client via a bridge.

### Install

```bash
pip install "kafklient[mcp]"
```

### Server example (`FastMCP` + `run_server`)

```python
import logging

from fastmcp import FastMCP

from kafklient.mcp.server import run_server

logging.basicConfig(level=logging.INFO)

mcp = FastMCP("My Kafka MCP Server")


@mcp.tool()
def echo(message: str) -> str:
    return f"Echo: {message}"


if __name__ == "__main__":
    run_server(
        mcp,
        bootstrap_servers="127.0.0.1:9092",
        consumer_topic="mcp-requests",
        producer_topic="mcp-responses",
        consumer_group_id="mcp-server",
        auto_create_topics=True,
        # Default: multi_session=True (session isolation mode)
        multi_session=True,
        show_banner=False,
        log_level="info",
    )
```

### CLI: server (`kafklient mcp-server`)

The CLI can load a `FastMCP` instance from a module or a Python file and run it over Kafka.

Supported `--mcp` formats:

- Module: `mypkg.myserver:mcp` (or `mypkg.myserver:mcp.some_attr`)
- File: `./myserver.py:mcp` (or `./myserver.py:mcp.some_attr`)
- If `:` is omitted, it defaults to `:mcp` (e.g. `mypkg.myserver` == `mypkg.myserver:mcp`)

```bash
# Module-based
kafklient mcp-server --mcp mypkg.myserver:mcp --bootstrap-servers 127.0.0.1:9092

# File-based
kafklient mcp-server --mcp ./myserver.py:mcp --bootstrap-servers 127.0.0.1:9092
```

### CLI: stdio <-> Kafka bridge (`kafklient mcp-client`)

```bash
uv run kafklient mcp-client --bootstrap-servers 127.0.0.1:9092
```

Useful flags:

- `--consumer-topic`: topic to read responses/notifications from (default: `mcp-responses`)
- `--producer-topic`: topic to write requests to (default: `mcp-requests`)
- `--isolate-session` / `--no-isolate-session`: isolate sessions by filtering responses using `x-session-id` (default: true)

## Development

Lint/type-check:

```bash
uv run ruff check .
uv run pyright
```

If Kafka is running (e.g. after `docker compose up -d`), run the tests:

```bash
uv run kafklient-test
```

## License

MIT

