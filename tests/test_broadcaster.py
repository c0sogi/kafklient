import asyncio
import unittest
from typing import AsyncIterator, Generic, TypeVar

from kafklient import Broadcaster, Callback

T = TypeVar("T")


class QueueStream(Generic[T]):
    """Simple in-memory async iterator to drive the Broadcaster in tests."""

    def __init__(self) -> None:
        self.queue: asyncio.Queue[T] = asyncio.Queue()
        self.subscription_count: int = 0
        self._subscription_event: asyncio.Event = asyncio.Event()

    async def listener(self) -> AsyncIterator[T]:
        self.subscription_count += 1
        self._subscription_event.set()

        async def generator() -> AsyncIterator[T]:
            while True:
                item = await self.queue.get()
                yield item

        return generator()

    async def publish(self, item: T) -> None:
        await self.queue.put(item)

    async def wait_for_subscription(self, expected: int, timeout: float = 1.0) -> None:
        async def _wait() -> None:
            while self.subscription_count < expected:
                self._subscription_event.clear()
                await self._subscription_event.wait()

        await asyncio.wait_for(_wait(), timeout=timeout)


class TestBroadcaster(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.stream: QueueStream[str] = QueueStream()
        self.broadcaster: Broadcaster[str] = Broadcaster(name="test-broker", listener=self.stream.listener)
        # Broadcaster builds the condition at import time; rebind it to this loop for tests.
        await self.broadcaster.start()
        await self.stream.wait_for_subscription(expected=1)

    async def asyncTearDown(self) -> None:
        await self.broadcaster.stop()

    async def test_wait_next_returns_new_items(self) -> None:
        initial_version = self.broadcaster.current_version
        waiter = asyncio.create_task(self.broadcaster.wait_next(initial_version))

        await asyncio.sleep(0.01)
        self.assertFalse(waiter.done(), "wait_next should block until a new item arrives")

        await self.stream.publish("alpha")
        result = await asyncio.wait_for(waiter, timeout=1.0)

        self.assertEqual(result, "alpha")
        self.assertGreater(self.broadcaster.current_version, initial_version)

    async def test_callback_invoked_for_each_item(self) -> None:
        received: list[str] = []
        done = asyncio.Event()

        async def collector(item: str, _: Callback[str]) -> None:
            received.append(item)
            if len(received) >= 2:
                done.set()

        self.broadcaster.register_callback(Callback(name="collector", callback=collector))

        await self.stream.publish("first")
        await self.stream.publish("second")
        await asyncio.wait_for(done.wait(), timeout=1.0)

        self.assertEqual(received, ["first", "second"])
        self.broadcaster.unregister_callback("collector")

    async def test_callback_errors_do_not_block_other_callbacks(self) -> None:
        failing_called = asyncio.Event()
        succeeding_called = asyncio.Event()

        async def failing_callback(_: str, __: Callback[str]) -> None:
            failing_called.set()
            raise RuntimeError("boom")

        async def succeeding_callback(_: str, __: Callback[str]) -> None:
            succeeding_called.set()

        self.broadcaster.register_callback(Callback(name="failing", callback=failing_callback))
        self.broadcaster.register_callback(Callback(name="succeeding", callback=succeeding_callback))

        await self.stream.publish("payload")
        await asyncio.wait_for(failing_called.wait(), timeout=1.0)
        await asyncio.wait_for(succeeding_called.wait(), timeout=1.0)

        self.broadcaster.unregister_callback("failing")
        self.broadcaster.unregister_callback("succeeding")

    async def test_stop_and_restart_consumes_items(self) -> None:
        version_before = self.broadcaster.current_version
        await self.stream.publish("one")
        first = await asyncio.wait_for(self.broadcaster.wait_next(version_before), timeout=1.0)
        self.assertEqual(first, "one")

        await self.broadcaster.stop()
        await self.broadcaster.start()
        await self.stream.wait_for_subscription(expected=2)

        version_after_restart = self.broadcaster.current_version
        waiter = asyncio.create_task(self.broadcaster.wait_next(version_after_restart))
        await self.stream.publish("two")
        second = await asyncio.wait_for(waiter, timeout=1.0)

        self.assertEqual(second, "two")

    async def test_start_is_idempotent(self) -> None:
        # Starting again should not create a new subscription
        await self.broadcaster.start()
        await asyncio.sleep(0.05)
        self.assertEqual(self.stream.subscription_count, 1)

        version = self.broadcaster.current_version
        waiter = asyncio.create_task(self.broadcaster.wait_next(version))
        await self.stream.publish("beta")
        result = await asyncio.wait_for(waiter, timeout=1.0)

        self.assertEqual(result, "beta")
