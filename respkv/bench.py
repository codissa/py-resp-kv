"""Multithreaded / multiprocess load generator (a small redis-benchmark).

Each client is one thread with its own TCP connection, issuing batches of
`--pipeline` commands and timing each round trip. Threads alone are enough
while clients are mostly blocked in socket calls (which release the GIL).
Encoding and parsing replies in Python hold the GIL, though, so at high rates
one process becomes the bottleneck. --processes spreads the clients over
several interpreters to get around it.

    python -m respkv.bench -p 6380 -c 50 -n 200000 -P 16
    python -m respkv.bench -p 6379 -c 50 -n 200000 -P 16   # compare with real Redis
"""

from __future__ import annotations

import argparse
import os
import random
import statistics
import threading
import time
from concurrent.futures import ProcessPoolExecutor

from .client import Client
from .protocol import Error, encode_command


def _client_thread(host, port, requests, pipeline, keyspace, value, get_ratio, seed, out, errors):
    rng = random.Random(seed)
    latencies = []
    try:
        with Client(host, port) as c:
            remaining = requests
            while remaining > 0:
                batch = min(pipeline, remaining)
                payload = bytearray()
                for _ in range(batch):
                    key = b"key:%d" % rng.randrange(keyspace)
                    if rng.random() < get_ratio:
                        payload += encode_command((b"GET", key))
                    else:
                        payload += encode_command((b"SET", key, value))
                t0 = time.perf_counter()
                c.send_raw(bytes(payload))
                for _ in range(batch):
                    if isinstance(c.read_reply(), Error):
                        errors.append(1)
                latencies.append(time.perf_counter() - t0)
                remaining -= batch
    except OSError as e:
        errors.append(e)
    out.extend(latencies)


def _run_process(
    host, port, clients, requests_per_client, pipeline, keyspace, value_size, get_ratio, seed
):
    value = os.urandom(value_size // 2 + 1).hex()[:value_size].encode()
    latencies: list[float] = []
    errors: list = []
    threads = [
        threading.Thread(
            target=_client_thread,
            args=(
                host,
                port,
                requests_per_client,
                pipeline,
                keyspace,
                value,
                get_ratio,
                seed * 1000 + i,
                latencies,
                errors,
            ),
        )
        for i in range(clients)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return latencies, len(errors)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="respkv.bench")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("-p", "--port", type=int, default=6380)
    p.add_argument("-c", "--clients", type=int, default=50, help="total concurrent connections")
    p.add_argument("-n", "--requests", type=int, default=100_000, help="total requests")
    p.add_argument("-P", "--pipeline", type=int, default=1, help="commands per round trip")
    p.add_argument("--processes", type=int, default=1, help="client processes (bypass the GIL)")
    p.add_argument("-r", "--keyspace", type=int, default=10_000)
    p.add_argument("-d", "--value-size", type=int, default=32)
    p.add_argument("--get-ratio", type=float, default=0.8)
    args = p.parse_args(argv)

    procs = max(1, min(args.processes, args.clients))
    per_proc = [args.clients // procs + (i < args.clients % procs) for i in range(procs)]
    per_client = max(1, args.requests // args.clients)
    total = per_client * args.clients

    t0 = time.perf_counter()
    with ProcessPoolExecutor(max_workers=procs) as pool:
        futures = [
            pool.submit(
                _run_process,
                args.host,
                args.port,
                n,
                per_client,
                args.pipeline,
                args.keyspace,
                args.value_size,
                args.get_ratio,
                i,
            )
            for i, n in enumerate(per_proc)
        ]
        results = [f.result() for f in futures]
    elapsed = time.perf_counter() - t0

    latencies = sorted(l for lat, _ in results for l in lat)
    errors = sum(e for _, e in results)
    if not latencies:
        raise SystemExit("no successful requests (is the server running?)")

    def pct(q: float) -> float:
        return latencies[min(len(latencies) - 1, int(q * len(latencies)))] * 1e3

    print(
        f"clients={args.clients} processes={procs} pipeline={args.pipeline} "
        f"get_ratio={args.get_ratio} value={args.value_size}B"
    )
    print(f"requests      {total}")
    print(f"elapsed       {elapsed:.2f} s")
    print(f"throughput    {total / elapsed:,.0f} ops/s")
    print(
        f"batch latency p50={pct(0.50):.3f} ms  p99={pct(0.99):.3f} ms  "
        f"max={latencies[-1] * 1e3:.3f} ms  mean={statistics.fmean(latencies) * 1e3:.3f} ms"
    )
    if errors:
        print(f"errors        {errors}")


if __name__ == "__main__":
    main()
