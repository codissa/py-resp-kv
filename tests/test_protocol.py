import pytest

from respkv.protocol import (
    NEED_MORE,
    Error,
    Map,
    Parser,
    ProtocolError,
    SimpleString,
    encode,
    encode_command,
)


def parse_all(data: bytes) -> list:
    p = Parser()
    p.feed(data)
    return list(p)


@pytest.mark.parametrize(
    "wire, value",
    [
        (b"+OK\r\n", "OK"),
        (b"-ERR bad\r\n", "ERR bad"),
        (b":-42\r\n", -42),
        (b"$5\r\nhello\r\n", b"hello"),
        (b"$0\r\n\r\n", b""),
        (b"$-1\r\n", None),
        (b"*-1\r\n", None),
        (b"*0\r\n", []),
        (b"*2\r\n$3\r\nfoo\r\n:7\r\n", [b"foo", 7]),
        (b"*2\r\n*1\r\n+a\r\n*0\r\n", [["a"], []]),
    ],
)
def test_parse_types(wire, value):
    assert parse_all(wire) == [value]


def test_simple_and_error_types_are_distinguished():
    ok, err = parse_all(b"+OK\r\n-ERR x\r\n")
    assert isinstance(ok, SimpleString) and not isinstance(ok, Error)
    assert isinstance(err, Error)


def test_bulk_strings_are_binary_safe():
    payload = b"\r\n\x00\xff$*"
    assert parse_all(encode(payload)) == [payload]


def test_inline_commands():
    assert parse_all(b"SET  foo   bar\r\nPING\r\n") == [[b"SET", b"foo", b"bar"], [b"PING"]]


def test_pipelined_commands_split_at_every_byte():
    """TCP may deliver any fragmentation. Feeding the stream in every possible
    split must yield identical messages."""
    cmds = [[b"SET", b"k", b"v" * 10], [b"GET", b"k"], [b"DEL", b"a", b"b"]]
    stream = b"".join(encode_command(c) for c in cmds)
    for i in range(len(stream) + 1):
        p = Parser()
        got = []
        for chunk in (stream[:i], stream[i:]):
            p.feed(chunk)
            got.extend(p)
        assert got == cmds, f"split at {i}"


def test_byte_by_byte_feed():
    stream = encode_command([b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"]) * 3
    p = Parser()
    got = []
    for byte in stream:
        p.feed(bytes([byte]))
        msg = p.get()
        if msg is not NEED_MORE:
            got.append(msg)
    assert len(got) == 3
    assert p.offset == len(stream)
    assert p.pending == 0


def test_need_more_until_complete():
    p = Parser()
    p.feed(b"$10\r\nhello")
    assert p.get() is NEED_MORE
    p.feed(b"world\r\n")
    assert p.get() == b"helloworld"


@pytest.mark.parametrize(
    "bad",
    [
        b"$abc\r\n",
        b":12x\r\n",
        b"$3\r\nhelloXX",  # wrong terminator
        b"$-5\r\n",
        b"*-2\r\n",
    ],
)
def test_protocol_errors(bad):
    with pytest.raises(ProtocolError):
        parse_all(bad)


def test_limits():
    p = Parser(max_bulk=4)
    p.feed(b"$5\r\n")
    with pytest.raises(ProtocolError):
        p.get()
    p = Parser()
    p.feed(b"x" * (70 * 1024))  # unterminated inline line
    with pytest.raises(ProtocolError):
        p.get()


def test_encode():
    assert encode(SimpleString("OK")) == b"+OK\r\n"
    assert encode(Error("ERR x")) == b"-ERR x\r\n"
    assert encode(5) == b":5\r\n"
    assert encode(None) == b"$-1\r\n"
    assert encode([b"a", 1, None]) == b"*3\r\n$1\r\na\r\n:1\r\n$-1\r\n"
    assert encode_command(["SET", "k", 10]) == b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\n10\r\n"
    with pytest.raises(TypeError):
        encode(True)


def test_compaction_keeps_offsets_correct():
    p = Parser()
    one = encode_command([b"SET", b"k", b"x" * 1000])
    total = 0
    for _ in range(200):  # > 64 KiB, forces compaction
        p.feed(one)
        total += len(one)
        assert p.get() == [b"SET", b"k", b"x" * 1000]
    assert p.offset == total


def test_encode_resp3_vs_resp2():
    m = Map({b"k": 1})
    assert encode(m) == b"*2\r\n$1\r\nk\r\n:1\r\n"  # RESP2: flat array
    assert encode(m, resp3=True) == b"%1\r\n$1\r\nk\r\n:1\r\n"  # RESP3: map
    assert encode(None, resp3=True) == b"_\r\n"
    assert encode([None], resp3=True) == b"*1\r\n_\r\n"
