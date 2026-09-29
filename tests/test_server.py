"""End-to-end tests over real TCP sockets."""

import socket
import threading
import time

import pytest

from respkv.client import Client, ReplyError
from respkv.protocol import Error, encode_command


def test_ping_echo(client):
    assert client.execute("PING") == "PONG"
    assert client.execute("ECHO", "hi") == b"hi"


def test_errors_are_replies_not_disconnects(client):
    with pytest.raises(ReplyError, match="unknown command"):
        client.execute("NOPE")
    assert client.execute("PING") == "PONG"


def test_pipelining_many_commands_in_one_write(client):
    cmds = [("INCR", "ctr") for _ in range(1000)] + [("GET", "ctr")]
    replies = client.pipeline(cmds)
    assert replies[:1000] == list(range(1, 1001))
    assert replies[-1] == b"1000"


def test_inline_protocol(server):
    with socket.create_connection(("127.0.0.1", server.port)) as s:
        s.sendall(b"SET greeting hello\r\nGET greeting\r\n")
        data = b""
        while data.count(b"\r\n") < 3:
            data += s.recv(1024)
        assert data == b"+OK\r\n$5\r\nhello\r\n"


def test_fragmented_request(server):
    with server.client() as c:
        payload = encode_command(["SET", "frag", "x" * 5000])
        for i in range(0, len(payload), 7):  # dribble the bytes out
            c.send_raw(payload[i : i + 7])
        assert c.read_reply() == "OK"
        assert c.execute("STRLEN", "frag") == 5000


def test_protocol_error_closes_connection(server):
    with server.client() as c:
        c.send_raw(b"*1\r\n$abc\r\n")
        reply = c.read_reply()
        assert isinstance(reply, Error) and "Protocol error" in reply
        with pytest.raises((ConnectionError, OSError)):
            c.execute("PING")


def test_quit(server):
    with server.client() as c:
        assert c.execute("QUIT") == "OK"
        with pytest.raises((ConnectionError, OSError)):
            c.execute("PING")


def test_concurrent_incr_is_atomic(server):
    """Many threads hammering one counter: the single-threaded event loop must
    serialize every INCR, so no update is lost."""
    threads, per_thread = 16, 300

    def worker():
        with server.client() as c:
            for _ in range(per_thread // 3):
                c.pipeline([("INCR", "counter")] * 3)

    ts = [threading.Thread(target=worker) for _ in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    with server.client() as c:
        assert c.execute("GET", "counter") == str(threads * per_thread).encode()


def test_concurrent_clients_isolated_keys(server):
    errors = []

    def worker(i):
        try:
            with server.client() as c:
                for j in range(50):
                    c.execute("RPUSH", f"list:{i}", j)
                expected = [str(j).encode() for j in range(50)]
                assert c.execute("LRANGE", f"list:{i}", 0, -1) == expected
        except Exception as e:  # pragma: no cover - reported below
            errors.append(e)

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errors


def test_active_expiry_runs_in_background(server, client):
    client.execute("SET", "tmp", "v", "PX", 50)
    deadline = time.time() + 3
    # Never read the key: only the server cron can remove it.
    while server.call(lambda: len(server.server.store)) and time.time() < deadline:
        time.sleep(0.05)
    assert server.call(lambda: len(server.server.store)) == 0


def test_info(client):
    client.execute("SET", "a", "1")
    info = client.execute("INFO").decode()
    assert "connected_clients:1" in info
    assert "db0:keys=1,expires=0" in info


def test_many_idle_connections(server):
    """Event-loop servers handle many mostly-idle connections cheaply."""
    clients = [Client("127.0.0.1", server.port) for _ in range(200)]
    try:
        for i, c in enumerate(clients):
            c.execute("SET", f"k{i}", i)
        for i, c in enumerate(clients):
            assert c.execute("GET", f"k{i}") == str(i).encode()
    finally:
        for c in clients:
            c.close()


@pytest.mark.parametrize("protocol", [2, 3])
def test_stock_redis_py_client(server, protocol):
    """The official client connects (HELLO handshake) and works on both protocols."""
    redis = pytest.importorskip("redis")
    r = redis.Redis(port=server.port, protocol=protocol)
    try:
        assert r.ping()
        r.set("k", "v", ex=100)
        assert r.get("k") == b"v"
        r.hset("h", mapping={"a": "1", "b": "2"})
        assert r.hgetall("h") == {b"a": b"1", b"b": b"2"}
        pipe = r.pipeline(transaction=False)
        for _ in range(10):
            pipe.incr("n")
        assert pipe.execute()[-1] == 10
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            r.lpush("k", "x")
    finally:
        r.close()
