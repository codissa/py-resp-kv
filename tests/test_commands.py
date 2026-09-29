"""Command semantics, run directly against the store with a fake clock."""

import pytest

from respkv.commands import Context, execute
from respkv.protocol import Error
from respkv.store import Store


@pytest.fixture
def run(clock):
    ctx = Context(Store(clock))

    def run(*args):
        return execute(ctx, [a if isinstance(a, bytes) else str(a).encode() for a in args])

    run.ctx = ctx
    return run


def test_get_set_del(run):
    assert run("GET", "k") is None
    assert run("SET", "k", "v") == "OK"
    assert run("GET", "k") == b"v"
    assert run("EXISTS", "k", "k", "nope") == 2
    assert run("DEL", "k", "nope") == 1
    assert run("GET", "k") is None


def test_set_nx_xx_get(run):
    assert run("SET", "k", "1", "XX") is None
    assert run("SET", "k", "1", "NX") == "OK"
    assert run("SET", "k", "2", "NX") is None
    assert run("SET", "k", "3", "XX", "GET") == b"1"
    assert run("GET", "k") == b"3"
    assert isinstance(run("SET", "k", "v", "NX", "XX"), Error)
    assert isinstance(run("SET", "k", "v", "EX", "0"), Error)
    assert isinstance(run("SET", "k", "v", "BOGUS"), Error)


def test_set_ex_expires_lazily(run, clock):
    run("SET", "k", "v", "EX", 10)
    assert run("TTL", "k") == 10
    clock.advance(9.5)
    assert run("PTTL", "k") == 500
    clock.advance(0.5)
    assert run("GET", "k") is None
    assert run("TTL", "k") == -2


def test_set_clears_ttl_unless_keepttl(run, clock):
    run("SET", "k", "v", "PX", 1000)
    run("SET", "k", "v2", "KEEPTTL")
    assert run("PTTL", "k") == 1000
    run("SET", "k", "v3")
    assert run("TTL", "k") == -1


def test_expire_persist(run, clock):
    assert run("EXPIRE", "missing", 5) == 0
    run("SET", "k", "v")
    assert run("EXPIRE", "k", 5) == 1
    assert run("PERSIST", "k") == 1
    assert run("PERSIST", "k") == 0
    clock.advance(10)
    assert run("GET", "k") == b"v"
    assert run("EXPIRE", "k", -1) == 1  # past deadline deletes
    assert run("EXISTS", "k") == 0


def test_active_expire_evicts_unread_keys(run, clock):
    store = run.ctx.store
    for i in range(100):
        run("SET", f"k{i}", "v", "PX", 100 + i)
    run("SET", "forever", "v")
    clock.advance(0.1505)  # 150.5 ms: avoids float rounding at the boundary
    assert store.active_expire() == 51  # k0..k50 have deadlines <= now
    assert len(store) == 50
    clock.advance(1)
    assert store.active_expire() == 49
    assert len(store) == 1
    assert store.expired_keys == 100


def test_active_expire_ignores_stale_heap_entries(run, clock):
    store = run.ctx.store
    run("SET", "k", "v", "PX", 100)
    run("PEXPIRE", "k", 10_000)  # heap now holds a stale 100ms entry
    clock.advance(1)
    assert store.active_expire() == 0
    assert run("GET", "k") == b"v"


def test_heap_is_rebuilt_when_mostly_stale(run):
    store = run.ctx.store
    run("SET", "k", "v")
    for i in range(5000):
        run("PEXPIRE", "k", 100_000 + i)
    assert store.heap_size < 2000


def test_incr_family(run):
    assert run("INCR", "n") == 1
    assert run("INCRBY", "n", 10) == 11
    assert run("DECR", "n") == 10
    assert run("DECRBY", "n", 20) == -10
    run("SET", "s", "abc")
    assert run("INCR", "s") == Error("ERR value is not an integer or out of range")
    run("SET", "big", str(2**63 - 1))
    assert isinstance(run("INCR", "big"), Error)
    assert isinstance(run("INCRBY", "n", "1.5"), Error)


def test_incr_keeps_ttl(run):
    run("SET", "n", "1", "EX", 100)
    run("INCR", "n")
    assert run("TTL", "n") == 100


def test_wrongtype(run):
    run("LPUSH", "l", "a")
    assert run("GET", "l").startswith("WRONGTYPE")
    assert run("HSET", "l", "f", "v").startswith("WRONGTYPE")
    assert run("TYPE", "l") == "list"
    assert run("TYPE", "nope") == "none"


def test_lists(run):
    assert run("RPUSH", "l", "a", "b", "c") == 3
    assert run("LPUSH", "l", "x", "y") == 5  # y x a b c
    assert run("LRANGE", "l", 0, -1) == [b"y", b"x", b"a", b"b", b"c"]
    assert run("LRANGE", "l", -2, 100) == [b"b", b"c"]
    assert run("LRANGE", "l", 3, 1) == []
    assert run("LPOP", "l") == b"y"
    assert run("RPOP", "l", 2) == [b"c", b"b"]
    assert run("LLEN", "l") == 2
    run("LPOP", "l", 5)
    assert run("EXISTS", "l") == 0  # empty lists are removed


def test_hashes(run):
    assert run("HSET", "h", "a", "1", "b", "2") == 2
    assert run("HSET", "h", "a", "9") == 0
    assert run("HGET", "h", "a") == b"9"
    assert run("HLEN", "h") == 2
    assert run("HINCRBY", "h", "b", 5) == 7
    assert run("HGETALL", "h") == {b"a": b"9", b"b": b"7"}
    assert run("HDEL", "h", "a", "b", "zz") == 2
    assert run("EXISTS", "h") == 0
    assert isinstance(run("HINCRBY", "new", "f", "x"), Error)
    assert run("EXISTS", "new") == 0  # a failed command must not create the key


def test_keys_pattern(run, clock):
    for k in ("user:1", "user:2", "session:1"):
        run("SET", k, "v")
    run("SET", "user:3", "v", "PX", 10)
    clock.advance(1)
    assert sorted(run("KEYS", "user:*")) == [b"user:1", b"user:2"]
    assert run("DBSIZE") >= 3


def test_hello_negotiates_protocol(run):
    assert run("HELLO")["proto"] == 2
    assert run("HELLO", 3)["proto"] == 3
    assert run.ctx.protocol == 3
    assert run("HELLO", 4).startswith("NOPROTO")
    assert run.ctx.protocol == 3


def test_arity_and_unknown(run):
    assert run("GET").startswith("ERR wrong number of arguments")
    assert run("NOPE").startswith("ERR unknown command")


def test_propagation_rewrites_relative_ttls(run, clock):
    now_ms = int(clock() * 1000)
    run("SET", "k", "v", "EX", 5)
    assert run.ctx.propagated == [
        [b"SET", b"k", b"v"],
        [b"PEXPIREAT", b"k", str(now_ms + 5000).encode()],
    ]
    run("EXPIRE", "k", 1)
    assert run.ctx.propagated == [[b"PEXPIREAT", b"k", str(now_ms + 1000).encode()]]
    run("INCR", "n")
    assert run.ctx.propagated == [[b"INCR", b"n"]]
    run("GET", "n")
    assert run.ctx.propagated == []
    run("INCR", "k")  # error: nothing to log
    assert run.ctx.propagated == []
    run("SET", "k", "v", "NX")  # no-op: nothing to log
    assert run.ctx.propagated == []
