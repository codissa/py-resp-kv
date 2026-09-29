"""asyncio TCP server speaking RESP2, compatible with redis-cli and redis-py.

Concurrency model (the same as Redis): one event-loop thread owns all the data.
Commands run one at a time to completion, so every command is atomic
without any locks, while thousands of connections are multiplexed with
non-blocking I/O. Blocking work (disk fsync, AOF rewrite) is pushed to
background threads so it never stalls the loop.

Pipelining: every complete command in a socket read is executed, the replies
are batched into one buffer and sent with a single write. The AOF is flushed
before that write, so a client never sees an acknowledgement for a write that
has not reached the OS.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import time
from collections.abc import Callable

from . import aof as aof_mod
from .commands import Context, execute
from .protocol import OK, Error, Parser, ProtocolError, SimpleString, encode
from .store import Store

log = logging.getLogger("respkv")


class Server:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        *,
        aof_path: str | None = None,
        appendfsync: str = "everysec",
        clock: Callable[[], float] = time.time,
        hz: int = 10,
    ) -> None:
        self.host, self.port = host, port
        self.store = Store(clock)
        self.aof_path = aof_path
        self.appendfsync = appendfsync
        self.aof: aof_mod.AppendOnlyFile | None = None
        self.hz = hz

        self.started_at = time.time()
        self.total_connections = 0
        self.total_commands = 0
        self._writers: set[asyncio.StreamWriter] = set()
        self._server: asyncio.base_events.Server | None = None
        self._cron: asyncio.Task | None = None
        self._rewrite_task: asyncio.Task | None = None

    # ------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        if self.aof_path:
            ctx = Context(self.store)
            n = aof_mod.load(self.aof_path, lambda argv: execute(ctx, argv))
            log.info("loaded %d commands from %s (%d keys)", n, self.aof_path, len(self.store))
            self.aof = aof_mod.AppendOnlyFile(self.aof_path, self.appendfsync)

        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        self._cron = asyncio.create_task(self._server_cron())
        log.info("listening on %s:%d", self.host, self.port)

    async def serve_forever(self) -> None:
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def close(self) -> None:
        if self._cron:
            self._cron.cancel()
        if self._server:
            self._server.close()
        for w in list(self._writers):
            w.close()
        if self._server:
            await self._server.wait_closed()
        if self._rewrite_task:
            await asyncio.gather(self._rewrite_task, return_exceptions=True)
        if self.aof:
            self.aof.close()
            self.aof = None

    async def _server_cron(self) -> None:
        """Periodic housekeeping, like Redis's serverCron (runs `hz` times/s)."""
        while True:
            await asyncio.sleep(1 / self.hz)
            self.store.active_expire(max_keys=1000)

    # ---------------------------------------------------------- connections
    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._writers.add(writer)
        self.total_connections += 1
        parser = Parser()
        ctx = Context(self.store, client_id=self.total_connections)
        try:
            while True:
                data = await reader.read(64 * 1024)
                if not data:
                    break
                parser.feed(data)

                out = bytearray()
                closing = False
                try:
                    for msg in parser:
                        if msg == []:  # blank inline line
                            continue
                        if not isinstance(msg, list):
                            raise ProtocolError("expected an array of bulk strings")
                        reply, closing = self._dispatch(ctx, msg)
                        out += encode(reply, resp3=ctx.protocol == 3)
                        if closing:
                            break
                except ProtocolError as e:
                    out += encode(Error(f"ERR Protocol error: {e}"))
                    closing = True

                if self.aof:
                    self.aof.flush()
                writer.write(out)
                await writer.drain()
                if closing:
                    break
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            self._writers.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    def _dispatch(self, ctx: Context, argv: list) -> tuple[object, bool]:
        self.total_commands += 1
        name = argv[0].lower() if isinstance(argv[0], bytes) else b""
        if name == b"quit":
            return OK, True
        if name == b"info":
            return self._info(), False
        if name == b"bgrewriteaof":
            return self._bgrewriteaof(), False

        reply = execute(ctx, argv)
        if self.aof is not None:
            for cmd in ctx.propagated:
                self.aof.append(cmd)
        return reply, False

    # ------------------------------------------------------ admin commands
    def _bgrewriteaof(self) -> object:
        if self.aof is None:
            return Error("ERR AOF is disabled")
        if self.aof.rewriting:
            return Error("ERR Background append only file rewriting already in progress")

        # Snapshot and begin_rewrite happen in the same loop tick, so every
        # write is in exactly one of: the snapshot, or the rewrite buffer.
        snapshot = aof_mod.snapshot_store(self.store.items())
        self.aof.begin_rewrite()
        self._rewrite_task = asyncio.create_task(self._run_rewrite(self.aof, snapshot))
        return SimpleString("Background append only file rewriting started")

    async def _run_rewrite(self, aof: aof_mod.AppendOnlyFile, snapshot) -> None:
        loop = asyncio.get_running_loop()
        tmp = None
        try:
            tmp = await loop.run_in_executor(None, aof.write_snapshot, snapshot)
            aof.finish_rewrite(tmp)
            log.info("AOF rewrite complete: %d keys", len(snapshot))
        except Exception:
            log.exception("AOF rewrite failed")
            aof.abort_rewrite(tmp)

    def _info(self) -> bytes:
        s = self.store
        lines = [
            "# Server",
            "redis_mode:standalone",
            "server:respkv",
            f"process_id:{os.getpid()}",
            f"tcp_port:{self.port}",
            f"uptime_in_seconds:{int(time.time() - self.started_at)}",
            "# Clients",
            f"connected_clients:{len(self._writers)}",
            "# Stats",
            f"total_connections_received:{self.total_connections}",
            f"total_commands_processed:{self.total_commands}",
            f"expired_keys:{s.expired_keys}",
            "# Persistence",
            f"aof_enabled:{int(self.aof is not None)}",
        ]
        if self.aof:
            lines += [
                f"aof_fsync_policy:{self.aof.fsync_policy}",
                f"aof_rewrite_in_progress:{int(self.aof.rewriting)}",
                f"aof_rewrites:{self.aof.rewrites}",
                f"aof_fsyncs:{self.aof.fsyncs}",
                f"aof_bytes_written:{self.aof.bytes_written}",
            ]
        lines += ["# Keyspace", f"db0:keys={len(s)},expires={s.expires_count}"]
        return ("\r\n".join(lines) + "\r\n").encode()


# --------------------------------------------------------------------- CLI
async def _serve(args: argparse.Namespace) -> None:
    server = Server(args.host, args.port, aof_path=args.aof, appendfsync=args.appendfsync)
    await server.start()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):  # Windows
            pass

    serve = asyncio.create_task(server.serve_forever())
    try:
        await stop.wait()
    finally:
        serve.cancel()
        await server.close()
        log.info("bye")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="respkv", description="Redis-compatible in-memory KV server")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6380)
    p.add_argument("--aof", metavar="PATH", help="enable append-only file persistence")
    p.add_argument("--appendfsync", choices=aof_mod.FSYNC_POLICIES, default="everysec")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(_serve(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
