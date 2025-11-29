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
        consumer_config={
            "bootstrap.servers": "127.0.0.1:9092",
            "auto.offset.reset": "latest",
        },
        auto_create_topics=True,
    ) as listener:
        stream = await listener.subscribe(dict[str, object])
        async for item in stream:
            print("got:", item)


asyncio.run(main())
