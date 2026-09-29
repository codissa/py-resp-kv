"""In-memory keyspace with Redis-style key expiry.

Expiry is handled two ways, as in Redis:

* Lazily: every read checks the key's deadline and deletes it if it has passed.
* Actively: ``active_expire()`` runs periodically and evicts expired keys that
  nobody reads, so they do not sit in memory forever. Redis samples random keys.
  Here a min-heap of (deadline, key) gives O(log n) eviction in deadline order.
  Heap entries are invalidated lazily. An entry is only acted on if it still
  matches the key's current deadline, so EXPIRE/PERSIST/SET never need to
  search the heap.
"""

from __future__ import annotations

import fnmatch
import heapq
import time
from collections import deque
from collections.abc import Callable, Iterator

Value = bytes | deque | dict


class Store:
    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._data: dict[bytes, Value] = {}
        self._expires: dict[bytes, int] = {}  # key -> absolute deadline, unix ms
        self._heap: list[tuple[int, bytes]] = []
        self.expired_keys = 0  # stat: keys removed because their TTL passed

    # ------------------------------------------------------------------ time
    def now_ms(self) -> int:
        return int(self._clock() * 1000)

    # ---------------------------------------------------------------- access
    def _expire_if_needed(self, key: bytes) -> bool:
        """Delete key if its deadline has passed. Returns True if it was deleted."""
        deadline = self._expires.get(key)
        if deadline is not None and deadline <= self.now_ms():
            del self._data[key]
            del self._expires[key]
            self.expired_keys += 1
            return True
        return False

    def get(self, key: bytes) -> Value | None:
        if self._expire_if_needed(key):
            return None
        return self._data.get(key)

    def __contains__(self, key: bytes) -> bool:
        return self.get(key) is not None

    def set(self, key: bytes, value: Value, *, keep_ttl: bool = False) -> None:
        self._data[key] = value
        if not keep_ttl:
            self._expires.pop(key, None)

    def delete(self, key: bytes) -> bool:
        if self._expire_if_needed(key):
            return False
        if key in self._data:
            del self._data[key]
            self._expires.pop(key, None)
            return True
        return False

    def flush(self) -> None:
        self._data.clear()
        self._expires.clear()
        self._heap.clear()

    def __len__(self) -> int:
        return len(self._data)

    @property
    def expires_count(self) -> int:
        return len(self._expires)

    def keys(self, pattern: bytes = b"*") -> list[bytes]:
        now = self.now_ms()
        match_all = pattern == b"*"
        return [
            k
            for k in self._data
            if (self._expires.get(k, now + 1) > now)
            and (match_all or fnmatch.fnmatchcase(k, pattern))
        ]

    def items(self) -> Iterator[tuple[bytes, Value, int | None]]:
        """Yield (key, value, deadline_ms) for all live keys."""
        now = self.now_ms()
        for k, v in self._data.items():
            deadline = self._expires.get(k)
            if deadline is None or deadline > now:
                yield k, v, deadline

    # ------------------------------------------------------------------- TTL
    def expire_at(self, key: bytes, deadline_ms: int) -> bool:
        """Set an absolute deadline. A deadline in the past deletes the key."""
        if self.get(key) is None:
            return False
        if deadline_ms <= self.now_ms():
            self.delete(key)
            return True
        self._expires[key] = deadline_ms
        heapq.heappush(self._heap, (deadline_ms, key))
        self._maybe_rebuild_heap()
        return True

    def persist(self, key: bytes) -> bool:
        if self.get(key) is None:
            return False
        return self._expires.pop(key, None) is not None

    def deadline(self, key: bytes) -> int | None:
        if self.get(key) is None:
            return None
        return self._expires.get(key)

    def pttl(self, key: bytes) -> int:
        """Redis semantics: -2 if missing, -1 if no TTL, else remaining ms."""
        if self.get(key) is None:
            return -2
        deadline = self._expires.get(key)
        if deadline is None:
            return -1
        return max(0, deadline - self.now_ms())

    def active_expire(self, max_keys: int = 1000) -> int:
        """Evict up to max_keys expired keys. Returns the number evicted."""
        now = self.now_ms()
        evicted = 0
        heap = self._heap
        while heap and heap[0][0] <= now and evicted < max_keys:
            deadline, key = heapq.heappop(heap)
            # Skip stale entries: the key's TTL was changed or removed since.
            if self._expires.get(key) == deadline:
                del self._data[key]
                del self._expires[key]
                self.expired_keys += 1
                evicted += 1
        return evicted

    def _maybe_rebuild_heap(self) -> None:
        # Stale entries accumulate when TTLs are overwritten. Rebuild once they
        # dominate, so memory stays O(live TTLs).
        if len(self._heap) > 2 * len(self._expires) + 1024:
            self._heap = [(d, k) for k, d in self._expires.items()]
            heapq.heapify(self._heap)

    @property
    def heap_size(self) -> int:
        return len(self._heap)
