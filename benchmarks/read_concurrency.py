"""Measure local RediSQL read throughput at several concurrency levels."""

from __future__ import annotations

import argparse
import csv
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import redis

from redisql import RediSQL

_FIELDS = (
    "concurrency",
    "requests",
    "errors",
    "elapsed_seconds",
    "rps",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
)
_SAMPLES_PER_WORKER = 10_000
_QUERY = "select value, label from benchmark_items where id = ?"


@dataclass
class Result:
    concurrency: int
    requests: int
    errors: int
    elapsed_seconds: float
    rps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    error_messages: tuple[str, ...]

    def row(self) -> dict[str, int | float]:
        return {field: getattr(self, field) for field in _FIELDS}


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _concurrency_levels(value: str) -> tuple[int, ...]:
    try:
        levels = tuple(_positive_int(item.strip()) for item in value.split(","))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected comma-separated positive integers") from exc
    if not levels:
        raise argparse.ArgumentTypeError("at least one level is required")
    return levels


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def _run_phase(
    db: RediSQL, rows: int, concurrency: int, duration: float, collect_samples: bool
) -> Result:
    ready = threading.Barrier(concurrency + 1)
    start = threading.Event()
    deadline_ns = 0

    def worker(worker_number: int) -> tuple[int, int, list[int], tuple[str, ...]]:
        nonlocal deadline_ns
        item_id = worker_number % rows + 1
        requests = errors = 0
        latencies: list[int] = []
        messages: list[str] = []
        ready.wait()
        start.wait()
        while time.perf_counter_ns() < deadline_ns:
            before = time.perf_counter_ns()
            try:
                db.query(_QUERY, (item_id,))
                requests += 1
            except Exception as exc:
                errors += 1
                if len(messages) < 3:
                    messages.append(f"{type(exc).__name__}: {exc}")
            if collect_samples and len(latencies) < _SAMPLES_PER_WORKER:
                latencies.append(time.perf_counter_ns() - before)
            item_id += 1
            if item_id > rows:
                item_id = 1
        return requests, errors, latencies, tuple(messages)

    with ThreadPoolExecutor(
        max_workers=concurrency, thread_name_prefix="redisql-benchmark"
    ) as pool:
        futures = [pool.submit(worker, worker_number) for worker_number in range(concurrency)]
        ready.wait()
        started_ns = time.perf_counter_ns()
        deadline_ns = started_ns + int(duration * 1_000_000_000)
        start.set()
        outcomes = [future.result() for future in futures]
    elapsed = (time.perf_counter_ns() - started_ns) / 1_000_000_000
    requests = sum(outcome[0] for outcome in outcomes)
    errors = sum(outcome[1] for outcome in outcomes)
    latencies = sorted(latency for outcome in outcomes for latency in outcome[2])
    messages = tuple(message for outcome in outcomes for message in outcome[3])

    def percentile(fraction: float) -> float:
        if not latencies:
            return 0.0
        index = max(0, round(len(latencies) * fraction) - 1)
        return latencies[index] / 1_000_000

    return Result(
        concurrency=concurrency,
        requests=requests,
        errors=errors,
        elapsed_seconds=elapsed,
        rps=requests / elapsed if elapsed else 0.0,
        p50_ms=percentile(0.50),
        p95_ms=percentile(0.95),
        p99_ms=percentile(0.99),
        max_ms=latencies[-1] / 1_000_000 if latencies else 0.0,
        error_messages=messages,
    )


def benchmark(
    db: RediSQL,
    rows: int,
    levels: tuple[int, ...],
    duration: float,
    warmup: float,
) -> list[Result]:
    results = []
    for concurrency in levels:
        if warmup:
            _run_phase(db, rows, concurrency, warmup, collect_samples=False)
        results.append(_run_phase(db, rows, concurrency, duration, collect_samples=True))
    return results


def _seed(db: RediSQL, rows: int) -> None:
    with db.batch() as conn:
        conn.execute(
            "create table benchmark_items("
            "id integer primary key, value integer not null, label text not null)"
        )
        conn.executemany(
            "insert into benchmark_items(value, label) values (?, ?)",
            ((number % 100, f"item-{number:08d}") for number in range(1, rows + 1)),
        )


def _write_results(results: list[Result], output: Path | None) -> None:
    stream = output.open("w", newline="") if output else sys.stdout
    try:
        writer = csv.DictWriter(stream, fieldnames=_FIELDS)
        writer.writeheader()
        for result in results:
            writer.writerow(
                {
                    **result.row(),
                    "elapsed_seconds": f"{result.elapsed_seconds:.3f}",
                    "rps": f"{result.rps:.2f}",
                    "p50_ms": f"{result.p50_ms:.3f}",
                    "p95_ms": f"{result.p95_ms:.3f}",
                    "p99_ms": f"{result.p99_ms:.3f}",
                    "max_ms": f"{result.max_ms:.3f}",
                }
            )
    finally:
        if output:
            stream.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark concurrent local reads from a RediSQL in-memory database."
    )
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--concurrency", type=_concurrency_levels, default=(1, 4, 16))
    parser.add_argument("--duration", type=_positive_float, default=5.0)
    parser.add_argument("--warmup", type=_nonnegative_float, default=1.0)
    parser.add_argument("--rows", type=_positive_int, default=100_000)
    parser.add_argument("--output", type=Path, help="Write CSV to this file instead of stdout")
    return parser


def main() -> int:
    args = _parser().parse_args()
    name = f"redisql-read-benchmark:{uuid.uuid4().hex}"
    client = redis.Redis.from_url(args.redis_url, socket_timeout=5)
    db = RediSQL(client, name)
    connected = False
    try:
        client.ping()
        connected = True
        db.start(listen=False)
        _seed(db, args.rows)
        results = benchmark(db, args.rows, args.concurrency, args.duration, args.warmup)
        _write_results(results, args.output)
        for result in results:
            for message in result.error_messages:
                print(
                    f"concurrency={result.concurrency} error: {message}",
                    file=sys.stderr,
                )
        return 1 if any(result.errors for result in results) else 0
    finally:
        db.close()
        if connected:
            client.delete(f"{name}:snap", f"{name}:ver", f"{name}:chan")
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
