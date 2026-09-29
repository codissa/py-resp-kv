"""RESP2 (REdis Serialization Protocol) incremental parser and encoder.

The parser is fed raw bytes as they arrive from a socket and yields complete
messages. It never blocks and never assumes a message arrives in a single
read, so it handles TCP fragmentation and pipelining (many commands in one read)
the same way.

Wire types:
    +OK\\r\\n                    simple string
    -ERR message\\r\\n           error
    :1000\\r\\n                  integer
    $5\\r\\nhello\\r\\n           bulk string     ($-1\\r\\n = null)
    *2\\r\\n$3\\r\\nGET\\r\\n...   array           (*-1\\r\\n = null)
RESP3 (negotiated per connection with HELLO 3) adds, among others, maps
(``%<n>``) and a dedicated null (``_``). Replies use those when the client asked for them.
Clients may also send "inline" commands: a plain line such as ``PING\\r\\n``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

__all__ = [
    "NEED_MORE",
    "Error",
    "Map",
    "Parser",
    "ProtocolError",
    "SimpleString",
    "encode",
    "encode_command",
]

CRLF = b"\r\n"
_SIMPLE, _ERROR, _INT, _BULK, _ARRAY = b"+-:$*"

MAX_INLINE = 64 * 1024
MAX_BULK = 512 * 1024 * 1024
MAX_MULTIBULK = 1024 * 1024


class ProtocolError(Exception):
    """The peer sent bytes that are not valid RESP."""


class SimpleString(str):
    """Reply that should be encoded as a RESP simple string (+...)."""


class Error(str):
    """Reply that should be encoded as a RESP error (-...)."""


OK = SimpleString("OK")
PONG = SimpleString("PONG")


class _NeedMore:
    def __repr__(self) -> str:
        return "NEED_MORE"


NEED_MORE: Any = _NeedMore()


class Parser:
    """Incremental RESP parser.

    >>> p = Parser()
    >>> p.feed(b"*1\\r\\n$4\\r\\nPI")
    >>> p.get() is NEED_MORE
    True
    >>> p.feed(b"NG\\r\\n")
    >>> p.get()
    [b'PING']
    """

    def __init__(self, *, max_bulk: int = MAX_BULK, max_multibulk: int = MAX_MULTIBULK) -> None:
        self._buf = bytearray()
        self._pos = 0
        self._discarded = 0  # bytes dropped from the front of _buf by compaction
        self._max_bulk = max_bulk
        self._max_multibulk = max_multibulk

    def feed(self, data: bytes) -> None:
        self._buf += data

    @property
    def offset(self) -> int:
        """Total bytes consumed by complete messages since creation."""
        return self._discarded + self._pos

    @property
    def pending(self) -> int:
        """Bytes buffered but not yet part of a complete message."""
        return len(self._buf) - self._pos

    def get(self) -> Any:
        """Return the next complete message, or NEED_MORE."""
        result = self._parse(self._pos)
        if result is None:
            return NEED_MORE
        value, self._pos = result
        self._compact()
        return value

    def __iter__(self):
        while True:
            msg = self.get()
            if msg is NEED_MORE:
                return
            yield msg

    def _compact(self) -> None:
        if self._pos == len(self._buf):
            self._discarded += self._pos
            self._buf.clear()
            self._pos = 0
        elif self._pos > 64 * 1024:
            self._discarded += self._pos
            del self._buf[: self._pos]
            self._pos = 0

    def _readline(self, pos: int) -> tuple[bytes, int] | None:
        end = self._buf.find(CRLF, pos)
        if end == -1:
            if len(self._buf) - pos > MAX_INLINE:
                raise ProtocolError("line too long")
            return None
        return bytes(self._buf[pos:end]), end + 2

    def _parse_int(self, raw: bytes) -> int:
        try:
            return int(raw)
        except ValueError:
            raise ProtocolError(f"invalid integer {raw[:32]!r}") from None

    def _parse(self, pos: int) -> tuple[Any, int] | None:
        buf = self._buf
        if pos >= len(buf):
            return None
        kind = buf[pos]

        if kind not in (_SIMPLE, _ERROR, _INT, _BULK, _ARRAY):
            # Inline command, e.g. "SET foo bar\r\n" typed into telnet.
            line = self._readline(pos)
            if line is None:
                return None
            raw, nxt = line
            return [bytes(tok) for tok in raw.split()], nxt

        line = self._readline(pos + 1)
        if line is None:
            return None
        raw, nxt = line

        if kind == _SIMPLE:
            return SimpleString(raw.decode()), nxt
        if kind == _ERROR:
            return Error(raw.decode()), nxt
        if kind == _INT:
            return self._parse_int(raw), nxt

        n = self._parse_int(raw)
        if kind == _BULK:
            if n == -1:
                return None, nxt
            if n < 0 or n > self._max_bulk:
                raise ProtocolError("invalid bulk length")
            end = nxt + n
            if end + 2 > len(buf):
                return None
            if buf[end : end + 2] != CRLF:
                raise ProtocolError("bulk string not terminated by CRLF")
            return bytes(buf[nxt:end]), end + 2

        # Array
        if n == -1:
            return None, nxt
        if n < 0 or n > self._max_multibulk:
            raise ProtocolError("invalid multibulk length")
        items = []
        for _ in range(n):
            res = self._parse(nxt)
            if res is None:
                return None
            item, nxt = res
            items.append(item)
        return items, nxt


class Map(dict):
    """Reply that is a map: a RESP3 map (%), or a flat key/value array in RESP2."""


Reply = int | bytes | str | list | tuple | dict | None


def encode(value: Reply, resp3: bool = False) -> bytes:
    """Encode a reply value as RESP2 bytes (or RESP3 if resp3 is True)."""
    out = bytearray()
    _encode_into(out, value, resp3)
    return bytes(out)


def _encode_into(out: bytearray, value: Any, resp3: bool) -> None:
    if value is None:
        out += b"_\r\n" if resp3 else b"$-1\r\n"
    elif isinstance(value, SimpleString):
        out += b"+%s\r\n" % value.encode()
    elif isinstance(value, Error):
        out += b"-%s\r\n" % value.encode()
    elif isinstance(value, bool):
        raise TypeError("bool is not a RESP2 type")
    elif isinstance(value, int):
        out += b":%d\r\n" % value
    elif isinstance(value, (bytes, bytearray, memoryview)):
        out += b"$%d\r\n" % len(value)
        out += value
        out += CRLF
    elif isinstance(value, str):
        _encode_into(out, value.encode(), resp3)
    elif isinstance(value, dict):
        if resp3:
            out += b"%%%d\r\n" % len(value)
        else:
            out += b"*%d\r\n" % (2 * len(value))
        for k, v in value.items():
            _encode_into(out, k, resp3)
            _encode_into(out, v, resp3)
    elif isinstance(value, (list, tuple)):
        out += b"*%d\r\n" % len(value)
        for item in value:
            _encode_into(out, item, resp3)
    else:
        raise TypeError(f"cannot encode {type(value).__name__}")


def _to_bytes(arg: Any) -> bytes:
    if isinstance(arg, bytes):
        return arg
    if isinstance(arg, str):
        return arg.encode()
    if isinstance(arg, (int, float)) and not isinstance(arg, bool):
        return str(arg).encode()
    raise TypeError(f"cannot send {type(arg).__name__} as a command argument")


def encode_command(args: Iterable[Any]) -> bytes:
    """Encode a client command as an array of bulk strings."""
    return encode([_to_bytes(a) for a in args])
