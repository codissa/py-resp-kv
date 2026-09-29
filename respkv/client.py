"""Minimal blocking RESP client (used by the tests and the benchmark)."""

from __future__ import annotations

import socket
from collections.abc import Iterable, Sequence
from typing import Any

from .protocol import NEED_MORE, Error, Parser, encode_command


class ReplyError(Exception):
    """The server answered with a RESP error."""


class Client:
    def __init__(self, host: str = "127.0.0.1", port: int = 6380, timeout: float | None = 10.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        # Disable Nagle: we want small request packets to go out immediately.
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.parser = Parser()

    def close(self) -> None:
        self.sock.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def send_raw(self, data: bytes) -> None:
        self.sock.sendall(data)

    def read_reply(self) -> Any:
        while True:
            reply = self.parser.get()
            if reply is not NEED_MORE:
                return reply
            chunk = self.sock.recv(64 * 1024)
            if not chunk:
                raise ConnectionError("server closed the connection")
            self.parser.feed(chunk)

    def execute(self, *args: Any) -> Any:
        self.sock.sendall(encode_command(args))
        reply = self.read_reply()
        if isinstance(reply, Error):
            raise ReplyError(str(reply))
        return reply

    def pipeline(self, commands: Iterable[Sequence[Any]]) -> list[Any]:
        """Send all commands in one write, then read all replies.

        Errors are returned in place (as protocol.Error), not raised."""
        cmds = list(commands)
        self.sock.sendall(b"".join(encode_command(c) for c in cmds))
        return [self.read_reply() for _ in cmds]
