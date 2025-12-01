"""
Kafka client performance benchmark.

Usage:
    uv run python -m tests.benchmark
    uv run python -m tests.benchmark --iterations 100 --warmup 10
"""

import argparse
import asyncio
import json
import statistics
import time
from dataclasses import dataclass

from kafklient import KafkaListener, KafkaRPC, Message, ParserSpec, create_producer

from ._config import KAFKA_BOOTSTRAP
from ._utils import (
    ensure_topic_exists,
    make_consumer_config,
    make_producer_config,
    make_ready_consumer,
)


@dataclass
class BenchmarkResult:
    name: str
    iterations: int
    latencies_ms: list[float]

    @property
    def mean(self) -> float:
        return statistics.mean(self.latencies_ms)

    @property
    def median(self) -> float:
        return statistics.median(self.latencies_ms)

    @property
    def stdev(self) -> float:
        return statistics.stdev(self.latencies_ms) if len(self.latencies_ms) > 1 else 0.0

    @property
    def min(self) -> float:
        return min(self.latencies_ms)

    @property
    def max(self) -> float:
        return max(self.latencies_ms)

    def percentile(self, p: float) -> float:
        """Calculate percentile (0-100)."""
        sorted_data = sorted(self.latencies_ms)
        k = (len(sorted_data) - 1) * p / 100
        f = int(k)
        c = f + 1 if f + 1 < len(sorted_data) else f
        return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])

    def print_stats(self) -> None:
        print(f"\n{'=' * 60}")
        print(f"  {self.name}")
        print(f"{'=' * 60}")
        print(f"  Iterations: {self.iterations}")
        print(f"  Mean:       {self.mean:8.2f} ms")
        print(f"  Median:     {self.median:8.2f} ms")
        print(f"  Std Dev:    {self.stdev:8.2f} ms")
        print(f"  Min:        {self.min:8.2f} ms")
        print(f"  Max:        {self.max:8.2f} ms")
        print(f"  P50:        {self.percentile(50):8.2f} ms")
        print(f"  P90:        {self.percentile(90):8.2f} ms")
        print(f"  P95:        {self.percentile(95):8.2f} ms")
        print(f"  P99:        {self.percentile(99):8.2f} ms")
        print(f"{'=' * 60}")


@dataclass
class SimpleRecord:
    value: str


async def benchmark_listener(iterations: int, warmup: int) -> BenchmarkResult:
    """Benchmark KafkaListener message receive latency."""
    topic = f"bench_listener_{int(time.time())}"
    group_id = f"bench_listener_group_{int(time.time())}"

    await ensure_topic_exists(topic)

    def parse_record(rec: Message) -> SimpleRecord:
        data = json.loads(rec.value() or b"{}")
        return SimpleRecord(value=str(data.get("value", "")))

    specs: list[ParserSpec[SimpleRecord]] = [{"topics": [topic], "type": SimpleRecord, "parser": parse_record}]

    listener = KafkaListener(
        parsers=specs,
        consumer_config=make_consumer_config(group_id),
    )

    # Reuse single producer for fair comparison
    producer = create_producer(make_producer_config())

    latencies: list[float] = []

    try:
        await listener.start()
        stream = await listener.subscribe(SimpleRecord)

        total = warmup + iterations
        print(f"  Running {warmup} warmup + {iterations} benchmark iterations...")

        for i in range(total):
            # Produce message with timestamp
            send_time = time.perf_counter()

            def produce_one() -> None:
                producer.produce(topic, value=json.dumps({"value": f"msg-{i}"}).encode())
                producer.flush()

            await asyncio.to_thread(produce_one)

            # Receive message
            async def receive() -> SimpleRecord | None:
                async for item in stream:
                    return item
                return None

            await asyncio.wait_for(receive(), timeout=10.0)
            recv_time = time.perf_counter()

            latency_ms = (recv_time - send_time) * 1000

            if i >= warmup:
                latencies.append(latency_ms)

            if (i + 1) % 20 == 0:
                print(f"    Progress: {i + 1}/{total}")

    finally:
        await listener.stop()

    return BenchmarkResult(
        name="KafkaListener (produce -> receive)",
        iterations=iterations,
        latencies_ms=latencies,
    )


async def benchmark_rpc(iterations: int, warmup: int) -> BenchmarkResult:
    """Benchmark KafkaRPC request/response latency."""
    request_topic = f"bench_rpc_req_{int(time.time())}"
    reply_topic = f"bench_rpc_rep_{int(time.time())}"
    client_group = f"bench_rpc_client_{int(time.time())}"
    server_group = f"bench_rpc_server_{int(time.time())}"

    await ensure_topic_exists(request_topic)
    await ensure_topic_exists(reply_topic)

    def parse_reply(rec: Message) -> bytes:
        return rec.value() or b""

    specs: list[ParserSpec[bytes]] = [{"topics": [reply_topic], "type": bytes, "parser": parse_reply}]

    rpc = KafkaRPC(
        parsers=specs,
        producer_config=make_producer_config(),
        consumer_config=make_consumer_config(client_group),
    )

    server_stop = asyncio.Event()
    server_ready = asyncio.Event()

    async def echo_server() -> None:
        consumer = await asyncio.to_thread(make_ready_consumer, server_group, [request_topic])
        producer = create_producer(make_producer_config())
        server_ready.set()

        def poll_one() -> Message | None:
            return consumer.poll(0.1)

        while not server_stop.is_set():
            msg = await asyncio.to_thread(poll_one)
            if msg is None or msg.error():
                continue

            corr_id: bytes | None = msg.key()
            if not corr_id:
                raise ValueError("Correlation ID is required")

            def send(reply_topic_name: str, reply_value: bytes, reply_headers: list[tuple[str, str | bytes]]) -> None:
                producer.produce(reply_topic_name, value=reply_value, headers=reply_headers)
                producer.flush()

            await asyncio.to_thread(
                send,
                reply_topic_name=next((v.decode() for k, v in (msg.headers() or ()) if k == "x-reply-topic")),
                reply_value=msg.value() or b"",
                reply_headers=[("x-corr-id", corr_id)],
            )

        consumer.close()

    server_task = asyncio.create_task(echo_server())
    latencies: list[float] = []

    try:
        await server_ready.wait()
        await rpc.start()

        total = warmup + iterations
        print(f"  Running {warmup} warmup + {iterations} benchmark iterations...")

        for i in range(total):
            start = time.perf_counter()
            await rpc.request(
                req_topic=request_topic,
                req_value=f"ping-{i}".encode(),
                req_headers_reply_to=[reply_topic],
                res_timeout=10.0,
                res_expect_type=bytes,
            )
            elapsed_ms = (time.perf_counter() - start) * 1000

            if i >= warmup:
                latencies.append(elapsed_ms)

            if (i + 1) % 20 == 0:
                print(f"    Progress: {i + 1}/{total}")

    finally:
        server_stop.set()
        await rpc.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass

    return BenchmarkResult(
        name="KafkaRPC (request -> response round-trip)",
        iterations=iterations,
        latencies_ms=latencies,
    )


async def run_benchmarks(iterations: int, warmup: int) -> None:
    print(f"\n{'#' * 60}")
    print("  Kafka Client Benchmark")
    print(f"  Bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"  Iterations: {iterations} (+ {warmup} warmup)")
    print(f"{'#' * 60}\n")

    print("Running Listener benchmark...")
    listener_result = await benchmark_listener(iterations, warmup)
    listener_result.print_stats()

    print("\nRunning RPC benchmark...")
    rpc_result = await benchmark_rpc(iterations, warmup)
    rpc_result.print_stats()

    print("\n" + "=" * 60)
    print("  Summary")
    print("=" * 60)
    print(f"  Listener avg: {listener_result.mean:.2f} ms (P99: {listener_result.percentile(99):.2f} ms)")
    print(f"  RPC avg:      {rpc_result.mean:.2f} ms (P99: {rpc_result.percentile(99):.2f} ms)")
    print("=" * 60 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka client performance benchmark")
    parser.add_argument("--iterations", "-n", type=int, default=200, help="Number of iterations")
    parser.add_argument("--warmup", "-w", type=int, default=50, help="Warmup iterations")
    args = parser.parse_args()

    asyncio.run(run_benchmarks(args.iterations, args.warmup))


if __name__ == "__main__":
    main()
