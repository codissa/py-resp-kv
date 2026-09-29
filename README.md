# respkv

[![CI](https://github.com/codissa/py-resp-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/codissa/py-resp-kv/actions/workflows/ci.yml)

A **Redis-compatible in-memory key-value server** written from scratch in pure Python (stdlib
only). It implements the RESP2/RESP3 wire protocol, an asyncio event-loop architecture,
TTL expiry, pipelining, and append-only-file persistence with background fsync and
background log compaction. Stock `redis-cli`, `redis-benchmark` and `redis-py` work against it
unmodified. CI checks all three.

```console
$ pip install -e .
$ respkv --port 6390 --aof appendonly.aof
$ redis-cli -p 6390
127.0.0.1:6390> SET session:42 alice EX 60
OK
127.0.0.1:6390> TTL session:42
(integer) 60
127.0.0.1:6390> HSET user:1 name Ada lang C
(integer) 2
127.0.0.1:6390> BGREWRITEAOF
Background append only file rewriting started
```

## Architecture

```
                ┌─────────────── event loop thread (owns ALL data) ───────────────┐
 clients ──TCP──▶ StreamReader ─▶ RESP Parser ─▶ execute() ─▶ Store ─▶ encode() ─┼─▶ one batched write
   (1000s)      │   (incremental,       │                │                         │
                │   pipelining)         │                └─▶ AOF buffer ─ write() ─┼─▶ appendonly.aof
                │                       │                     (before the reply)   │        ▲
                │  server cron (10 Hz) ─┴─▶ active expiry (min-heap)               │        │ fsync
                └──────────────────────────────────────────────────────────────────┘        │
                   aof-fsync thread (everysec) ─────────────────────────────────────────────┘
                   rewrite worker thread (BGREWRITEAOF) ──▶ snapshot ─▶ .rewrite.tmp ─▶ os.replace
```

### Why a single-threaded event loop?
This is the Redis model. One thread owns the keyspace and runs each command to completion, so
**every command is atomic without any locks**, and `INCR` from 16 threads never loses an update
(see `test_concurrent_incr_is_atomic`). Concurrency comes from non-blocking I/O multiplexing
(epoll/kqueue/IOCP through asyncio), not from threads, so thousands of mostly-idle connections
cost almost nothing. Threads are used only for **blocking work that must not stall the loop**:
the `fsync()` syscall and serializing the AOF rewrite.

### Protocol (`respkv/protocol.py`)
* An **incremental parser**. It is fed arbitrary TCP chunks and yields complete messages. The
  tests feed a pipelined stream split at *every byte offset* and byte-by-byte.
* It is binary-safe (bulk strings carry length prefixes), supports inline commands
  (`telnet`-style), and enforces size limits against memory-exhaustion attacks.
* **RESP3** is negotiated per connection with `HELLO 3`. Maps are encoded natively as `%`
  and null as `_`. `redis-py` 8 speaks RESP3 by default.

### Pipelining
Every complete command in a socket read is executed, and the replies are gathered into **one
buffer sent with one `write()`**. That removes a syscall and a round trip per command, and it is
why `-P 16` is 3-4x faster in the benchmark below.

### Expiry (`respkv/store.py`)
* **Lazy**: every access checks the deadline.
* **Active**: a 10 Hz cron evicts expired keys that nobody reads. Redis samples random keys.
  Here a **min-heap of `(deadline, key)`** evicts in deadline order in O(log n). Heap entries
  are invalidated *lazily*: an entry is acted on only if it still matches the key's current
  deadline, so `EXPIRE`/`PERSIST`/`SET` never have to search the heap. The heap is rebuilt when
  stale entries outnumber live ones 2:1, which keeps memory bounded.

### Persistence (`respkv/aof.py`)
* **Write-ahead ordering**: the AOF buffer is written to the OS *before* the batched reply is
  sent, so a client never gets an acknowledgement for a write that has not left the process.
* **fsync policy** as in Redis. `always` fsyncs before the reply. `everysec` fsyncs from a
  background thread, so the event loop never blocks on the disk and at most about 1 s of writes
  is lost on power failure. `no` leaves flushing to the OS.
* **Relative TTLs are logged as absolute** `PEXPIREAT`. Replaying the log hours later then
  expires keys at the right moment instead of restarting their timers.
* **Torn-write recovery**: if the process dies mid-append, the partial command at the end of
  the file is detected on load and truncated away.
* **`BGREWRITEAOF` compaction without stopping the world**:
  1. On the loop, take a point-in-time snapshot and start teeing new writes into a *rewrite
     buffer*.
  2. A worker thread serializes the snapshot to `.rewrite.tmp` and fsyncs it.
  3. Back on the loop, append the rewrite buffer, fsync, then atomically `os.replace` it over
     the live AOF and fsync the directory.

  Every write lands in exactly one of the snapshot or the rewrite buffer. That is tested by
  writing *during* a rewrite and then restarting.

## Commands

`PING ECHO HELLO SELECT CLIENT COMMAND CONFIG INFO QUIT BGREWRITEAOF`
`GET SET[EX|PX|EXAT|PXAT|NX|XX|KEEPTTL|GET] GETDEL MGET MSET APPEND STRLEN INCR DECR INCRBY DECRBY`
`DEL EXISTS TYPE KEYS DBSIZE FLUSHALL FLUSHDB EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST TTL PTTL`
`LPUSH RPUSH LPOP RPOP LLEN LRANGE` `HSET HGET HDEL HGETALL HLEN HEXISTS HINCRBY`

Error replies match Redis (`WRONGTYPE`, `ERR value is not an integer or out of range`, arity
errors, `NOPROTO`), and so do the edge cases: empty lists and hashes are deleted, `INCR` keeps
the TTL, and a failed command never creates a key.

## Benchmarks

The official `redis-benchmark` against `respkv --aof` (fsync everysec), CPython 3.12, 50
clients, in Docker on an 8-vCPU laptop. Numbers are indicative only.

| test | no pipelining | `-P 16` |
|---|---|---|
| SET | 17.1k req/s (p50 2.8 ms) | 57.2k req/s |
| GET | 23.5k req/s (p50 2.1 ms) | 97.8k req/s |
| INCR | 17.9k req/s | |
| LPUSH | 17.7k req/s | |

A bundled multithreaded, multiprocess load generator reports latency percentiles:

```console
$ python -m respkv.bench -p 6390 -c 50 -n 1000000 -P 32 --processes 4
```

The client threads spend most of their time blocked in `send`/`recv`, which release the GIL.
Encoding requests and parsing replies holds it, though, so one client process saturates
before the server does. `--processes` spreads clients over several interpreters.

## Tests

```console
$ pip install -e ".[dev]"
$ pytest            # 63 tests: protocol, commands, TCP end-to-end, persistence, redis-py
$ ruff check . && ruff format --check .
```

CI runs Python 3.10–3.13 on Linux, plus Windows and macOS. A separate job starts the server
and drives it with the real `redis-cli` and `redis-benchmark` binaries.

## License

MIT
