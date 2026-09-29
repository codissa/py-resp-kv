"""Append-only file (AOF) persistence.

Every write command is appended to the log in RESP format. On startup the log
is replayed to rebuild the keyspace.

Durability is controlled by the fsync policy, as in Redis:

* ``always``:   fsync before replying to the client. Durable, but slow.
* ``everysec``: a background thread fsyncs once per second, so the event loop
  never blocks on the disk. A crash can lose up to about 1 s of writes.
* ``no``:       leave flushing to the OS page cache.

Log compaction (BGREWRITEAOF) runs in a worker thread:

1. On the event loop, take a point-in-time copy of the keyspace. After that,
   every new write goes both to the live AOF and to an in-memory rewrite buffer.
2. In a worker thread, serialize the snapshot to ``<path>.rewrite.tmp`` and fsync it.
3. Back on the event loop, append the rewrite buffer to the temp file, fsync
   it, and atomically ``os.replace`` it over the live AOF.

The event loop is the only thread that mutates the keyspace or appends to the
log. The worker thread only reads its private snapshot, so no data locks are
needed. A single small lock protects the file descriptor against the fsync
thread while the files are swapped.
"""

from __future__ import annotations

import logging
import os
import threading
from collections import deque
from collections.abc import Callable, Iterable

from .protocol import NEED_MORE, Parser, ProtocolError, encode_command

log = logging.getLogger(__name__)

FSYNC_POLICIES = ("always", "everysec", "no")
_OPEN_FLAGS = os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_BINARY", 0)

Snapshot = list  # list of (key, value_copy, deadline_ms | None)


def _write_all(fd: int, data: bytes | bytearray) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def _fsync_dir(path: str) -> None:
    """Make a rename durable on POSIX by fsyncing the containing directory."""
    if not hasattr(os, "O_DIRECTORY"):
        return
    fd = os.open(os.path.dirname(os.path.abspath(path)), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class AppendOnlyFile:
    def __init__(self, path: str, fsync: str = "everysec") -> None:
        if fsync not in FSYNC_POLICIES:
            raise ValueError(f"fsync policy must be one of {FSYNC_POLICIES}")
        self.path = path
        self.fsync_policy = fsync
        self._fd = os.open(path, _OPEN_FLAGS, 0o644)
        self._fd_lock = threading.Lock()
        self._buf = bytearray()
        self._dirty = False
        self._rewrite_buf: bytearray | None = None

        self.fsyncs = 0
        self.rewrites = 0
        self.bytes_written = 0

        self._stop = threading.Event()
        self._fsync_thread: threading.Thread | None = None
        if fsync == "everysec":
            self._fsync_thread = threading.Thread(
                target=self._fsync_loop, name="aof-fsync", daemon=True
            )
            self._fsync_thread.start()

    # ------------------------------------------------------------ appending
    def append(self, argv: Iterable[bytes]) -> None:
        data = encode_command(argv)
        self._buf += data
        if self._rewrite_buf is not None:
            self._rewrite_buf += data

    def flush(self) -> None:
        """Write buffered commands to the OS. Call before sending replies."""
        if not self._buf:
            return
        _write_all(self._fd, self._buf)
        self.bytes_written += len(self._buf)
        self._buf.clear()
        self._dirty = True
        if self.fsync_policy == "always":
            self._fsync()

    def _fsync(self) -> None:
        with self._fd_lock:
            # Clear first: a write that lands during the fsync re-marks it dirty.
            self._dirty = False
            os.fsync(self._fd)
            self.fsyncs += 1

    def _fsync_loop(self) -> None:
        while not self._stop.wait(1.0):
            if self._dirty:
                try:
                    self._fsync()
                except OSError:
                    log.exception("background fsync failed")

    def close(self) -> None:
        self.flush()
        self._stop.set()
        if self._fsync_thread is not None:
            self._fsync_thread.join()
        with self._fd_lock:
            if self.fsync_policy != "no":
                os.fsync(self._fd)
            os.close(self._fd)

    # ------------------------------------------------------------ rewriting
    @property
    def rewriting(self) -> bool:
        return self._rewrite_buf is not None

    def begin_rewrite(self) -> None:
        """Called on the event loop at the instant the snapshot is taken."""
        if self._rewrite_buf is not None:
            raise RuntimeError("rewrite already in progress")
        self._rewrite_buf = bytearray()

    def write_snapshot(self, snapshot: Snapshot) -> str:
        """Runs in a worker thread. Returns the temp file path."""
        tmp = self.path + ".rewrite.tmp"
        with open(tmp, "wb") as f:
            for key, value, deadline in snapshot:
                for argv in _commands_for(key, value):
                    f.write(encode_command(argv))
                if deadline is not None:
                    f.write(encode_command([b"PEXPIREAT", key, str(deadline).encode()]))
            f.flush()
            os.fsync(f.fileno())
        return tmp

    def finish_rewrite(self, tmp: str) -> None:
        """Called on the event loop once write_snapshot() has returned."""
        assert self._rewrite_buf is not None
        self.flush()
        with open(tmp, "ab") as f:
            f.write(self._rewrite_buf)
            f.flush()
            os.fsync(f.fileno())
        with self._fd_lock:
            # Close before replacing: Windows cannot replace a file that is open.
            os.close(self._fd)
            os.replace(tmp, self.path)
            self._fd = os.open(self.path, _OPEN_FLAGS, 0o644)
            self._dirty = False
        _fsync_dir(self.path)
        self._rewrite_buf = None
        self.rewrites += 1

    def abort_rewrite(self, tmp: str | None = None) -> None:
        self._rewrite_buf = None
        if tmp and os.path.exists(tmp):
            os.remove(tmp)


def _commands_for(key: bytes, value) -> Iterable[list[bytes]]:
    if isinstance(value, bytes):
        yield [b"SET", key, value]
    elif isinstance(value, (list, deque)):
        items = list(value)
        for i in range(0, len(items), 512):
            yield [b"RPUSH", key, *items[i : i + 512]]
    elif isinstance(value, dict):
        pairs = [x for kv in value.items() for x in kv]
        for i in range(0, len(pairs), 1024):
            yield [b"HSET", key, *pairs[i : i + 1024]]
    else:
        raise TypeError(f"cannot serialize {type(value).__name__}")


def snapshot_store(items: Iterable[tuple[bytes, object, int | None]]) -> Snapshot:
    """Point-in-time copy. Containers are copied because the loop keeps mutating them."""
    out = []
    for key, value, deadline in items:
        if isinstance(value, deque):
            value = list(value)
        elif isinstance(value, dict):
            value = dict(value)
        out.append((key, value, deadline))
    return out


def load(path: str, apply: Callable[[list], object]) -> int:
    """Replay the AOF at path through apply(argv). Returns the command count.

    A partially written command at the end of the file (a crash mid-append)
    is truncated away with a warning, as Redis does with aof-load-truncated.
    """
    if not os.path.exists(path):
        return 0
    parser = Parser()
    count = 0
    with open(path, "rb") as f:
        while chunk := f.read(1 << 16):
            parser.feed(chunk)
            while True:
                try:
                    argv = parser.get()
                except ProtocolError as e:
                    raise ProtocolError(f"corrupt AOF at offset {parser.offset}: {e}") from e
                if argv is NEED_MORE:
                    break
                apply(argv)
                count += 1
    if parser.pending:
        log.warning(
            "AOF %s ends with a truncated command (%d bytes); truncating to offset %d",
            path,
            parser.pending,
            parser.offset,
        )
        os.truncate(path, parser.offset)
    return count
