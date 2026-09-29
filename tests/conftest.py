from __future__ import annotations

import asyncio
import threading

import pytest

from respkv.client import Client
from respkv.server import Server


class ServerThread:
    """Runs a Server on its own event loop in a background thread."""

    def __init__(self, **kwargs) -> None:
        kwargs.setdefault("port", 0)  # ephemeral port
        self.server = Server(**kwargs)
        self.loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.server.start())
        except BaseException as e:  # surface startup failures to the test
            self._error = e
            self._ready.set()
            return
        self._ready.set()
        self.loop.run_forever()
        self.loop.close()

    def start(self) -> ServerThread:
        self._thread.start()
        self._ready.wait(10)
        if self._error:
            raise self._error
        return self

    def call(self, fn):
        """Run fn() on the server's loop thread and return its result."""

        async def wrapper():
            return fn()

        return asyncio.run_coroutine_threadsafe(wrapper(), self.loop).result(10)

    def stop(self) -> None:
        asyncio.run_coroutine_threadsafe(self.server.close(), self.loop).result(10)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(10)

    @property
    def port(self) -> int:
        return self.server.port

    def client(self) -> Client:
        return Client("127.0.0.1", self.port)


@pytest.fixture
def server():
    st = ServerThread().start()
    yield st
    st.stop()


@pytest.fixture
def client(server):
    with server.client() as c:
        yield c


class FakeClock:
    def __init__(self, t: float = 1_700_000_000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def clock():
    return FakeClock()
