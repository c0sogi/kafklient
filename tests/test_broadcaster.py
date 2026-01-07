import asyncio
import unittest
from typing import AsyncIterator, Generic, TypeVar

from kafklient import Broadcaster, BroadcasterStoppedError, Callback

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

    async def test_wait_next_without_after_version_waits_for_next_publish(self) -> None:
        # publish one item to establish a non-zero version and latest item
        v0 = self.broadcaster.current_version
        await self.stream.publish("first")
        first = await asyncio.wait_for(self.broadcaster.wait_next(v0), timeout=1.0)
        self.assertEqual(first, "first")

        # Without after_version, wait_next should wait for the next publish (not return immediately).
        waiter = asyncio.create_task(self.broadcaster.wait_next())
        await asyncio.sleep(0.01)
        self.assertFalse(waiter.done(), "wait_next() should block until a new item arrives")

        await self.stream.publish("second")
        second = await asyncio.wait_for(waiter, timeout=1.0)
        self.assertEqual(second, "second")

    async def test_wait_next_without_after_version_raises_on_stop_before_first_item(self) -> None:
        waiter = asyncio.create_task(self.broadcaster.wait_next())
        await asyncio.sleep(0)

        await self.broadcaster.stop()

        with self.assertRaises(BroadcasterStoppedError):
            await asyncio.wait_for(waiter, timeout=1.0)

    async def test_wait_next_raises_when_stopped(self) -> None:
        v0 = self.broadcaster.current_version
        waiter = asyncio.create_task(self.broadcaster.wait_next(v0))
        await asyncio.sleep(0)

        await self.broadcaster.stop()

        with self.assertRaises(BroadcasterStoppedError):
            await asyncio.wait_for(waiter, timeout=1.0)

    async def test_wait_next_returns_available_item_even_if_stopped(self) -> None:
        v0 = self.broadcaster.current_version
        await self.stream.publish("alpha")
        first = await asyncio.wait_for(self.broadcaster.wait_next(v0), timeout=1.0)
        self.assertEqual(first, "alpha")

        # After stopping, wait_next should still return the latest item if the requested version is behind.
        await self.broadcaster.stop()
        second = await asyncio.wait_for(self.broadcaster.wait_next(v0), timeout=1.0)
        self.assertEqual(second, "alpha")

    async def test_callback_invoked_for_each_item(self) -> None:
        received: list[str] = []
        done = asyncio.Event()

        async def collector(item: str) -> None:
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

        async def failing_callback(_: str) -> None:
            failing_called.set()
            raise RuntimeError("boom")

        async def succeeding_callback(_: str) -> None:
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

    async def test_policy_exhaust_skips_while_running(self) -> None:
        started = asyncio.Event()
        blocker = asyncio.Event()
        calls: list[str] = []

        async def cb(item: str) -> None:
            calls.append(item)
            started.set()
            await blocker.wait()

        self.broadcaster.register_callback(Callback(name="exhaust", callback=cb, policy="exhaust"))

        await self.stream.publish("one")
        await asyncio.wait_for(started.wait(), timeout=1.0)

        # While callback is blocked, new items should be ignored.
        await self.stream.publish("two")
        await asyncio.sleep(0.05)
        self.assertEqual(calls, ["one"])

        blocker.set()
        await asyncio.sleep(0.05)
        self.broadcaster.unregister_callback("exhaust")

    async def test_policy_switch_cancels_previous_and_runs_latest(self) -> None:
        blocker = asyncio.Event()
        started_one = asyncio.Event()
        started_two = asyncio.Event()
        cancelled: list[str] = []
        completed: list[str] = []

        async def cb(item: str) -> None:
            if item == "one":
                started_one.set()
            if item == "two":
                started_two.set()
            try:
                await blocker.wait()
            except asyncio.CancelledError:
                cancelled.append(item)
                raise
            completed.append(item)

        self.broadcaster.register_callback(Callback(name="switch", callback=cb, policy="switch"))

        await self.stream.publish("one")
        await asyncio.wait_for(started_one.wait(), timeout=1.0)

        await self.stream.publish("two")
        await asyncio.wait_for(started_two.wait(), timeout=1.0)

        blocker.set()
        await asyncio.sleep(0.05)

        self.assertIn("one", cancelled)
        self.assertEqual(completed, ["two"])

        async def _wait_tasks_drained() -> None:
            while self.broadcaster._callback_tasks:  # type: ignore[reportPrivateUsage]
                await asyncio.sleep(0)

        await asyncio.wait_for(_wait_tasks_drained(), timeout=1.0)
        self.broadcaster.unregister_callback("switch")

    async def test_policy_concat_runs_sequentially(self) -> None:
        blocker = asyncio.Event()
        started_one = asyncio.Event()
        started_two = asyncio.Event()
        calls: list[str] = []

        async def cb(item: str) -> None:
            calls.append(item)
            if item == "one":
                started_one.set()
                await blocker.wait()
            if item == "two":
                started_two.set()

        self.broadcaster.register_callback(Callback(name="concat", callback=cb, policy="concat"))

        await self.stream.publish("one")
        await asyncio.wait_for(started_one.wait(), timeout=1.0)

        await self.stream.publish("two")
        await asyncio.sleep(0.05)
        self.assertFalse(started_two.is_set(), "concat should not start processing 'two' before 'one' completes")

        blocker.set()
        await asyncio.wait_for(started_two.wait(), timeout=1.0)
        self.assertEqual(calls, ["one", "two"])
        self.broadcaster.unregister_callback("concat")

    async def test_policy_concat_stop_does_not_hang_when_second_is_waiting(self) -> None:
        blocker = asyncio.Event()
        started_one = asyncio.Event()

        async def cb(item: str) -> None:
            if item == "one":
                started_one.set()
                await blocker.wait()

        self.broadcaster.register_callback(Callback(name="concat", callback=cb, policy="concat"))

        v0 = self.broadcaster.current_version
        await self.stream.publish("one")
        await asyncio.wait_for(started_one.wait(), timeout=1.0)
        await asyncio.wait_for(self.broadcaster.wait_next(v0), timeout=1.0)

        # Publish a second item so a second concat task is created and awaits the first task.
        await self.stream.publish("two")
        await asyncio.wait_for(self.broadcaster.wait_next(v0 + 1), timeout=1.0)
        await asyncio.sleep(0)

        # stop() should cancel pending concat tasks cleanly (no hang).
        await asyncio.wait_for(self.broadcaster.stop(), timeout=1.0)
