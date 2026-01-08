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

    async def test_register_callback_with_before_parameter(self) -> None:
        """Test registering callback with 'before' parameter."""
        call_order: list[str] = []

        async def cb1(item: str) -> None:
            call_order.append("cb1")

        async def cb2(item: str) -> None:
            call_order.append("cb2")

        async def cb3(item: str) -> None:
            call_order.append("cb3")

        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))
        # Register cb3 before cb1
        self.broadcaster.register_callback(Callback(name="cb3", callback=cb3), before="cb1")

        self.assertEqual(self.broadcaster.get_callback_order(), ["cb3", "cb1", "cb2"])

        await self.stream.publish("test")
        await asyncio.sleep(0.05)

        # Callbacks should be called in order: cb3, cb1, cb2
        self.assertEqual(call_order, ["cb3", "cb1", "cb2"])

        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")
        self.broadcaster.unregister_callback("cb3")

    async def test_register_callback_with_after_parameter(self) -> None:
        """Test registering callback with 'after' parameter."""
        call_order: list[str] = []

        async def cb1(item: str) -> None:
            call_order.append("cb1")

        async def cb2(item: str) -> None:
            call_order.append("cb2")

        async def cb3(item: str) -> None:
            call_order.append("cb3")

        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))
        # Register cb3 after cb1
        self.broadcaster.register_callback(Callback(name="cb3", callback=cb3), after="cb1")

        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb3", "cb2"])

        await self.stream.publish("test")
        await asyncio.sleep(0.05)

        # Callbacks should be called in order: cb1, cb3, cb2
        self.assertEqual(call_order, ["cb1", "cb3", "cb2"])

        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")
        self.broadcaster.unregister_callback("cb3")

    async def test_register_callback_before_after_validation(self) -> None:
        """Test that register_callback validates before/after parameters."""

        async def cb(item: str) -> None:
            pass

        # Cannot specify both before and after
        with self.assertRaises(ValueError, msg="Cannot specify both 'before' and 'after'"):
            self.broadcaster.register_callback(Callback(name="cb", callback=cb), before="other", after="another")

        # Reference callback must exist
        with self.assertRaises(ValueError, msg="Reference callback not found"):
            self.broadcaster.register_callback(Callback(name="cb", callback=cb), before="nonexistent")

    async def test_register_callback_before_after_self_is_rejected(self) -> None:
        """Regression: reject before/after pointing to the callback being registered/updated."""

        async def cb1(item: str) -> None:
            pass

        async def cb1_updated(item: str) -> None:
            pass

        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1"])

        with self.assertRaisesRegex(ValueError, "before.*itself"):
            self.broadcaster.register_callback(Callback(name="cb1", callback=cb1_updated), before="cb1")
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1"])

        with self.assertRaisesRegex(ValueError, "after.*itself"):
            self.broadcaster.register_callback(Callback(name="cb1", callback=cb1_updated), after="cb1")
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1"])

        self.broadcaster.unregister_callback("cb1")

    async def test_reorder_callback(self) -> None:
        """Test reordering existing callbacks."""
        call_order: list[str] = []

        async def cb1(item: str) -> None:
            call_order.append("cb1")

        async def cb2(item: str) -> None:
            call_order.append("cb2")

        async def cb3(item: str) -> None:
            call_order.append("cb3")

        # Register in order: cb1, cb2, cb3
        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))
        self.broadcaster.register_callback(Callback(name="cb3", callback=cb3))

        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2", "cb3"])

        # Reorder: move cb3 before cb1
        self.broadcaster.reorder_callback("cb3", before="cb1")
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb3", "cb1", "cb2"])

        call_order.clear()
        await self.stream.publish("test")
        await asyncio.sleep(0.05)
        self.assertEqual(call_order, ["cb3", "cb1", "cb2"])

        # Reorder: move cb2 after cb3
        self.broadcaster.reorder_callback("cb2", after="cb3")
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb3", "cb2", "cb1"])

        call_order.clear()
        await self.stream.publish("test2")
        await asyncio.sleep(0.05)
        self.assertEqual(call_order, ["cb3", "cb2", "cb1"])

        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")
        self.broadcaster.unregister_callback("cb3")

    async def test_reorder_callback_validation(self) -> None:
        """Test that reorder_callback validates parameters."""

        async def cb(item: str) -> None:
            pass

        self.broadcaster.register_callback(Callback(name="cb", callback=cb))

        # Cannot specify both before and after
        with self.assertRaises(ValueError, msg="Cannot specify both 'before' and 'after'"):
            self.broadcaster.reorder_callback("cb", before="other", after="another")

        # Callback must exist
        with self.assertRaises(ValueError, msg="Callback not found"):
            self.broadcaster.reorder_callback("nonexistent", before="cb")

        # Reference callback must exist
        with self.assertRaises(ValueError, msg="Reference callback not found"):
            self.broadcaster.reorder_callback("cb", before="nonexistent")

        self.broadcaster.unregister_callback("cb")

    async def test_reorder_callback_before_after_self_is_rejected(self) -> None:
        """Regression: reject before/after pointing to the callback being reordered."""

        async def cb1(item: str) -> None:
            pass

        async def cb2(item: str) -> None:
            pass

        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2"])

        with self.assertRaisesRegex(ValueError, "before.*itself"):
            self.broadcaster.reorder_callback("cb1", before="cb1")
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2"])

        with self.assertRaisesRegex(ValueError, "after.*itself"):
            self.broadcaster.reorder_callback("cb1", after="cb1")
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2"])

        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")

    async def test_set_callback_order(self) -> None:
        """Test setting callback order in bulk."""
        call_order: list[str] = []

        async def cb1(item: str) -> None:
            call_order.append("cb1")

        async def cb2(item: str) -> None:
            call_order.append("cb2")

        async def cb3(item: str) -> None:
            call_order.append("cb3")

        async def cb4(item: str) -> None:
            call_order.append("cb4")

        # Register callbacks
        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))
        self.broadcaster.register_callback(Callback(name="cb3", callback=cb3))
        self.broadcaster.register_callback(Callback(name="cb4", callback=cb4))

        # Set new order: cb4, cb2, cb1, cb3
        self.broadcaster.set_callback_order(["cb4", "cb2", "cb1", "cb3"])
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb4", "cb2", "cb1", "cb3"])

        call_order.clear()
        await self.stream.publish("test")
        await asyncio.sleep(0.05)
        self.assertEqual(call_order, ["cb4", "cb2", "cb1", "cb3"])

        # Set partial order: only cb3, cb1 (cb2 and cb4 should be appended in current order)
        self.broadcaster.set_callback_order(["cb3", "cb1"])
        # Current order was ["cb4", "cb2", "cb1", "cb3"], so omitted ones (cb4, cb2) keep their order
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb3", "cb1", "cb4", "cb2"])

        call_order.clear()
        await self.stream.publish("test2")
        await asyncio.sleep(0.05)
        self.assertEqual(call_order, ["cb3", "cb1", "cb4", "cb2"])

        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")
        self.broadcaster.unregister_callback("cb3")
        self.broadcaster.unregister_callback("cb4")

    async def test_set_callback_order_validation(self) -> None:
        """Test that set_callback_order validates parameters."""

        async def cb(item: str) -> None:
            pass

        self.broadcaster.register_callback(Callback(name="cb", callback=cb))

        # All specified callbacks must exist
        with self.assertRaises(ValueError, msg="Callback not found"):
            self.broadcaster.set_callback_order(["cb", "nonexistent"])

        self.broadcaster.unregister_callback("cb")

    async def test_get_callback_order(self) -> None:
        """Test getting callback order."""

        async def cb1(item: str) -> None:
            pass

        async def cb2(item: str) -> None:
            pass

        # Initially empty
        self.assertEqual(self.broadcaster.get_callback_order(), [])

        # Register callbacks
        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))

        order = self.broadcaster.get_callback_order()
        self.assertEqual(order, ["cb1", "cb2"])

        # Verify it's a copy (modifying shouldn't affect internal state)
        order.append("cb3")
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2"])

        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")

    async def test_callback_order_preserved_on_update(self) -> None:
        """Test that updating a callback preserves its order unless explicitly reordered."""
        call_order: list[str] = []

        async def cb1_v1(item: str) -> None:
            call_order.append("cb1_v1")

        async def cb1_v2(item: str) -> None:
            call_order.append("cb1_v2")

        async def cb2(item: str) -> None:
            call_order.append("cb2")

        # Register callbacks
        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1_v1))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))

        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2"])

        # Update cb1 without reordering - should preserve position
        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1_v2))
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2"])

        call_order.clear()
        await self.stream.publish("test")
        await asyncio.sleep(0.05)
        self.assertEqual(call_order, ["cb1_v2", "cb2"])

        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")

    async def test_unregister_callback_removes_from_order(self) -> None:
        """Test that unregistering removes callback from order."""

        async def cb1(item: str) -> None:
            pass

        async def cb2(item: str) -> None:
            pass

        async def cb3(item: str) -> None:
            pass

        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))
        self.broadcaster.register_callback(Callback(name="cb3", callback=cb3))

        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2", "cb3"])

        self.broadcaster.unregister_callback("cb2")
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb3"])

        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb3")
        self.assertEqual(self.broadcaster.get_callback_order(), [])

    async def test_unregister_callback_returns_true_if_present_in_callbacks_even_if_missing_in_order(self) -> None:
        """Regression: return value should reflect presence in _callbacks, not _callback_order."""

        async def cb1(item: str) -> None:
            pass

        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.assertIn("cb1", self.broadcaster._callbacks)  # type: ignore[reportPrivateUsage]
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1"])

        # Simulate edge case: callback exists but is missing from order list
        self.broadcaster._callback_order.remove("cb1")  # type: ignore[reportPrivateUsage]
        self.assertEqual(self.broadcaster.get_callback_order(), [])

        removed = self.broadcaster.unregister_callback("cb1")
        self.assertTrue(removed)
        self.assertNotIn("cb1", self.broadcaster._callbacks)  # type: ignore[reportPrivateUsage]

    async def test_callback_execution_order_with_different_policies(self) -> None:
        """Test that callbacks execute in order regardless of policy."""
        call_order: list[str] = []

        async def cb1(item: str) -> None:
            call_order.append("cb1")

        async def cb2(item: str) -> None:
            call_order.append("cb2")

        async def cb3(item: str) -> None:
            call_order.append("cb3")

        # Register with different policies
        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1, policy="merge"))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2, policy="concat"))
        self.broadcaster.register_callback(Callback(name="cb3", callback=cb3, policy="switch"))

        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2", "cb3"])

        await self.stream.publish("test")
        await asyncio.sleep(0.05)

        # All callbacks should be called in registration order
        self.assertEqual(call_order, ["cb1", "cb2", "cb3"])

        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")
        self.broadcaster.unregister_callback("cb3")

    async def test_reorder_callback_reference_not_in_order_list(self) -> None:
        """Test Bug 1: reorder_callback when reference callback is not in _callback_order."""

        async def cb1(item: str) -> None:
            pass

        async def cb2(item: str) -> None:
            pass

        # Register cb1 normally
        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        # Register cb2 but manually remove it from order list to simulate the bug scenario
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))
        self.broadcaster._callback_order.remove("cb2")  # type: ignore[reportPrivateUsage]

        # Now cb2 is in _callbacks but not in _callback_order
        # Try to reorder cb2 before cb1 - this should work
        self.broadcaster.reorder_callback("cb2", before="cb1")
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb2", "cb1"])

        # Register cb3 (also not in order list initially)
        self.broadcaster._callbacks["cb3"] = Callback(name="cb3", callback=cb1)  # type: ignore[reportPrivateUsage]

        # Try to reorder cb3 before cb2 where cb2 IS in _callback_order - should work
        self.broadcaster.reorder_callback("cb3", before="cb2")
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb3", "cb2", "cb1"])

        # Clean up
        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")
        self.broadcaster.unregister_callback("cb3")

    async def test_reorder_callback_reference_not_in_order_list_error(self) -> None:
        """Test Bug 1: reorder_callback should error when reference is not in _callback_order."""

        async def cb1(item: str) -> None:
            pass

        async def cb2(item: str) -> None:
            pass

        # Register both callbacks
        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))

        # Manually remove cb2 from order list to simulate edge case
        self.broadcaster._callback_order.remove("cb2")  # type: ignore[reportPrivateUsage]

        # Create a third callback not in order list
        self.broadcaster._callbacks["cb3"] = Callback(name="cb3", callback=cb1)  # type: ignore[reportPrivateUsage]

        # Try to reorder cb3 before cb2 (where cb2 is NOT in _callback_order)
        # This should raise a clear error
        with self.assertRaises(ValueError, msg="Reference callback not in callback order"):
            self.broadcaster.reorder_callback("cb3", before="cb2")

        # Clean up
        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")
        self.broadcaster.unregister_callback("cb3")

    async def test_set_callback_order_with_duplicates(self) -> None:
        """Test Bug 2: set_callback_order should reject duplicate names."""

        async def cb1(item: str) -> None:
            pass

        async def cb2(item: str) -> None:
            pass

        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))

        # Try to set order with duplicates - should raise ValueError
        with self.assertRaises(ValueError, msg="Duplicate names"):
            self.broadcaster.set_callback_order(["cb1", "cb1", "cb2"])

        # Verify order wasn't changed
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2"])

        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")

    async def test_set_callback_order_duplicates_dont_cause_multiple_execution(self) -> None:
        """Test Bug 2: Verify duplicates would have caused multiple executions (if not fixed)."""
        call_count: dict[str, int] = {"cb1": 0}

        async def cb1(item: str) -> None:
            call_count["cb1"] += 1

        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))

        # The bug fix prevents this, but verify the order is clean
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1"])

        await self.stream.publish("test")
        await asyncio.sleep(0.05)

        # Callback should only be called once
        self.assertEqual(call_count["cb1"], 1)

        self.broadcaster.unregister_callback("cb1")

    async def test_register_callback_reference_not_in_order_list(self) -> None:
        """Test that register_callback validates reference is in _callback_order."""

        async def cb1(item: str) -> None:
            pass

        async def cb2(item: str) -> None:
            pass

        # Register cb1 normally
        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))

        # Manually remove cb1 from order list to simulate edge case
        self.broadcaster._callback_order.remove("cb1")  # type: ignore[reportPrivateUsage]

        # Try to register cb2 before cb1 (where cb1 is NOT in _callback_order)
        with self.assertRaises(ValueError, msg="Reference callback not in callback order"):
            self.broadcaster.register_callback(Callback(name="cb2", callback=cb2), before="cb1")

        # Try to register cb2 after cb1 (where cb1 is NOT in _callback_order)
        with self.assertRaises(ValueError, msg="Reference callback not in callback order"):
            self.broadcaster.register_callback(Callback(name="cb2", callback=cb2), after="cb1")

        # Clean up
        self.broadcaster.unregister_callback("cb1")

    async def test_reorder_callback_in_order_list_reference_not_in_order(self) -> None:
        """Test that reorder_callback validates reference is in _callback_order (when callback is already in order)."""

        async def cb1(item: str) -> None:
            pass

        async def cb2(item: str) -> None:
            pass

        # Register both callbacks normally
        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))

        # Manually remove cb2 from order list
        self.broadcaster._callback_order.remove("cb2")  # type: ignore[reportPrivateUsage]

        # Try to reorder cb1 (which IS in order) before cb2 (which is NOT in order)
        with self.assertRaises(ValueError, msg="Reference callback not in callback order"):
            self.broadcaster.reorder_callback("cb1", before="cb2")

        # Try to reorder cb1 after cb2
        with self.assertRaises(ValueError, msg="Reference callback not in callback order"):
            self.broadcaster.reorder_callback("cb1", after="cb2")

        # Clean up
        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")

    async def test_register_callback_validation_failure_preserves_order(self) -> None:
        """Test Bug 1: register_callback validation failure should not remove callback from order."""

        async def cb1(item: str) -> None:
            pass

        async def cb2(item: str) -> None:
            pass

        # Register cb1 normally
        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1"])

        # Try to update cb1 with invalid before reference - should fail
        with self.assertRaises(ValueError):
            self.broadcaster.register_callback(Callback(name="cb1", callback=cb2), before="nonexistent")

        # cb1 should still be in the order list
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1"])
        self.assertIn("cb1", self.broadcaster._callbacks)  # type: ignore[reportPrivateUsage]

        # Verify cb1 is still executed
        call_order: list[str] = []

        async def cb_test(item: str) -> None:
            call_order.append("executed")

        self.broadcaster.register_callback(Callback(name="cb1", callback=cb_test))
        await self.stream.publish("test")
        await asyncio.sleep(0.05)
        self.assertEqual(call_order, ["executed"])

        # Clean up
        self.broadcaster.unregister_callback("cb1")

    async def test_reorder_callback_validation_failure_preserves_order(self) -> None:
        """Test Bug 1: reorder_callback validation failure should not remove callback from order."""

        async def cb1(item: str) -> None:
            pass

        async def cb2(item: str) -> None:
            pass

        # Register both callbacks
        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2"])

        # Try to reorder cb1 with invalid before reference - should fail
        with self.assertRaises(ValueError):
            self.broadcaster.reorder_callback("cb1", before="nonexistent")

        # cb1 should still be in the order list at original position
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2"])

        # Verify cb1 is still executed
        call_order: list[str] = []

        async def cb_test(item: str) -> None:
            call_order.append("cb1")

        async def cb2_test(item: str) -> None:
            call_order.append("cb2")

        self.broadcaster.register_callback(Callback(name="cb1", callback=cb_test))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2_test))
        await self.stream.publish("test")
        await asyncio.sleep(0.05)
        self.assertEqual(call_order, ["cb1", "cb2"])

        # Clean up
        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")

    async def test_register_callback_update_validation_failure_with_order_reference(self) -> None:
        """Test Bug 1: updating callback with invalid order reference should preserve callback in order."""

        async def cb1(item: str) -> None:
            pass

        async def cb2(item: str) -> None:
            pass

        async def cb3(item: str) -> None:
            pass

        # Register three callbacks
        self.broadcaster.register_callback(Callback(name="cb1", callback=cb1))
        self.broadcaster.register_callback(Callback(name="cb2", callback=cb2))
        self.broadcaster.register_callback(Callback(name="cb3", callback=cb3))
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2", "cb3"])

        # Manually remove cb3 from order to simulate edge case
        self.broadcaster._callback_order.remove("cb3")  # type: ignore[reportPrivateUsage]

        # Try to update cb2 to be before cb3 (which is not in order) - should fail
        with self.assertRaises(ValueError, msg="Reference callback not in callback order"):
            self.broadcaster.register_callback(Callback(name="cb2", callback=cb1), before="cb3")

        # cb2 should still be in the order list at original position
        self.assertEqual(self.broadcaster.get_callback_order(), ["cb1", "cb2"])

        # Clean up
        self.broadcaster.unregister_callback("cb1")
        self.broadcaster.unregister_callback("cb2")
        self.broadcaster.unregister_callback("cb3")
