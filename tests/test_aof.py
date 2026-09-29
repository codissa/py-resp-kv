"""Persistence: replay, truncated-tail recovery, and background rewrite."""

import os
import time

import pytest

from respkv.aof import load
from respkv.commands import Context, execute
from respkv.protocol import encode_command
from respkv.store import Store

from .conftest import ServerThread


@pytest.fixture
def aof_path(tmp_path):
    return str(tmp_path / "appendonly.aof")


def restart(aof_path, **kw):
    return ServerThread(aof_path=aof_path, **kw).start()


@pytest.mark.parametrize("policy", ["always", "everysec", "no"])
def test_data_survives_restart(aof_path, policy):
    st = restart(aof_path, appendfsync=policy)
    with st.client() as c:
        c.execute("SET", "s", "hello")
        c.execute("RPUSH", "l", "a", "b", "c")
        c.execute("HSET", "h", "f", "v")
        c.pipeline([("INCR", "n")] * 100)
        c.execute("SET", "gone", "x")
        c.execute("DEL", "gone")
        c.execute("SET", "ttl", "x", "EX", 1000)
    st.stop()

    st = restart(aof_path)
    with st.client() as c:
        assert c.execute("GET", "s") == b"hello"
        assert c.execute("LRANGE", "l", 0, -1) == [b"a", b"b", b"c"]
        assert c.execute("HGET", "h", "f") == b"v"
        assert c.execute("GET", "n") == b"100"
        assert c.execute("EXISTS", "gone") == 0
        assert 990 <= c.execute("TTL", "ttl") <= 1000  # absolute deadline preserved
    st.stop()


def test_expired_keys_stay_expired_after_restart(aof_path):
    st = restart(aof_path)
    with st.client() as c:
        c.execute("SET", "short", "x", "PX", 100)
        c.execute("SET", "long", "x")
    st.stop()
    time.sleep(0.2)
    st = restart(aof_path)
    with st.client() as c:
        assert c.execute("EXISTS", "short") == 0
        assert c.execute("EXISTS", "long") == 1
    st.stop()


def test_truncated_tail_is_recovered(aof_path):
    good = encode_command([b"SET", b"a", b"1"]) + encode_command([b"SET", b"b", b"2"])
    torn = encode_command([b"SET", b"c", b"3"])[:-5]  # crash mid-write
    with open(aof_path, "wb") as f:
        f.write(good + torn)

    ctx = Context(Store())
    assert load(aof_path, lambda argv: execute(ctx, argv)) == 2
    assert os.path.getsize(aof_path) == len(good)
    assert ctx.store.get(b"a") == b"1" and ctx.store.get(b"c") is None


def test_bgrewriteaof_compacts_and_keeps_concurrent_writes(aof_path):
    st = restart(aof_path)
    with st.client() as c:
        c.pipeline([("INCR", "n")] * 2000)
        c.pipeline([("RPUSH", "l", i) for i in range(1500)])
        c.execute("SET", "e", "v", "EX", 500)
        size_before = os.path.getsize(aof_path)

        assert c.execute("BGREWRITEAOF").startswith("Background")
        # Writes issued while the rewrite runs must not be lost.
        c.pipeline([("INCR", "during")] * 50)

        deadline = time.time() + 5
        while b"aof_rewrites:1" not in c.execute("INFO") and time.time() < deadline:
            time.sleep(0.02)
        assert b"aof_rewrites:1" in c.execute("INFO")
        c.execute("INCR", "after")
    size_after = os.path.getsize(aof_path)
    st.stop()

    assert size_after < size_before / 2
    assert not os.path.exists(aof_path + ".rewrite.tmp")

    st = restart(aof_path)
    with st.client() as c:
        assert c.execute("GET", "n") == b"2000"
        assert c.execute("LLEN", "l") == 1500
        assert c.execute("LRANGE", "l", 0, 2) == [b"0", b"1", b"2"]
        assert c.execute("GET", "during") == b"50"
        assert c.execute("GET", "after") == b"1"
        assert c.execute("TTL", "e") > 0
    st.stop()


def test_bgrewriteaof_without_aof_is_an_error(server, client):
    from respkv.client import ReplyError

    with pytest.raises(ReplyError, match="AOF is disabled"):
        client.execute("BGREWRITEAOF")
