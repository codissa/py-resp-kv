"""Command table and implementations.

Each command handler receives a Context and the argument list (without the
command name) and returns a reply value that protocol.encode() understands.

Write commands are propagated to the append-only file. By default the command
is logged exactly as received. Commands with relative TTLs (SET EX, EXPIRE, ...)
rewrite themselves into absolute PEXPIREAT so that replaying the log later
reproduces the same deadlines.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field

from . import __version__
from .protocol import OK, PONG, Error, Map, SimpleString
from .store import Store

WRONGTYPE = Error("WRONGTYPE Operation against a key holding the wrong kind of value")
NOT_INT = Error("ERR value is not an integer or out of range")
SYNTAX = Error("ERR syntax error")

INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1


class CommandError(Exception):
    """Raised by handlers to abort with an error reply."""

    def __init__(self, reply: Error) -> None:
        super().__init__(reply)
        self.reply = reply


@dataclass
class Context:
    store: Store
    propagated: list[list[bytes]] = field(default_factory=list)
    custom_propagation: bool = False
    protocol: int = 2  # RESP version negotiated with HELLO
    client_id: int = 0

    def propagate(self, *args: bytes) -> None:
        self.custom_propagation = True
        self.propagated.append(list(args))


Handler = Callable[[Context, list], object]


@dataclass(frozen=True)
class Command:
    name: str
    handler: Handler
    arity: int  # >0 exact arg count (incl. name), <0 minimum, like Redis
    write: bool


COMMANDS: dict[bytes, Command] = {}


def command(name: str, arity: int, write: bool = False):
    def register(fn: Handler) -> Handler:
        COMMANDS[name.encode().lower()] = Command(name, fn, arity, write)
        return fn

    return register


# ------------------------------------------------------------------ helpers
def parse_int(raw: bytes) -> int:
    try:
        value = int(raw)
    except ValueError:
        raise CommandError(NOT_INT) from None
    if not INT64_MIN <= value <= INT64_MAX or raw != str(value).encode():
        raise CommandError(NOT_INT)
    return value


def get_string(ctx: Context, key: bytes) -> bytes | None:
    value = ctx.store.get(key)
    if value is not None and not isinstance(value, bytes):
        raise CommandError(WRONGTYPE)
    return value


def get_list(ctx: Context, key: bytes, create: bool = False) -> deque | None:
    value = ctx.store.get(key)
    if value is None:
        if not create:
            return None
        value = deque()
        ctx.store.set(key, value)
    if not isinstance(value, deque):
        raise CommandError(WRONGTYPE)
    return value


def get_hash(ctx: Context, key: bytes, create: bool = False) -> dict | None:
    value = ctx.store.get(key)
    if value is None:
        if not create:
            return None
        value = {}
        ctx.store.set(key, value)
    if not isinstance(value, dict):
        raise CommandError(WRONGTYPE)
    return value


def b(n: int) -> bytes:
    return str(n).encode()


# --------------------------------------------------------------- connection
@command("PING", -1)
def cmd_ping(ctx, args):
    if len(args) > 1:
        raise CommandError(Error("ERR wrong number of arguments for 'ping' command"))
    return args[0] if args else PONG


@command("ECHO", 2)
def cmd_echo(ctx, args):
    return args[0]


@command("SELECT", 2)
def cmd_select(ctx, args):
    if args[0] != b"0":
        raise CommandError(Error("ERR DB index is out of range"))
    return OK


@command("HELLO", -1)
def cmd_hello(ctx, args):
    if args:
        try:
            version = int(args[0])
        except ValueError:
            raise CommandError(
                Error("ERR Protocol version is not an integer or out of range")
            ) from None
        if version not in (2, 3):
            raise CommandError(Error("NOPROTO unsupported protocol version"))
        ctx.protocol = version
    return Map(
        {
            "server": "respkv",
            "version": __version__,
            "proto": ctx.protocol,
            "id": ctx.client_id,
            "mode": "standalone",
            "role": "master",
            "modules": [],
        }
    )


@command("CONFIG", -2)
def cmd_config(ctx, args):
    # redis-benchmark and some clients probe CONFIG GET at startup. No
    # settings are exposed, so reply with an empty result.
    if args[0].upper() == b"GET":
        return Map()
    return OK


@command("CLIENT", -2)
def cmd_client(ctx, args):
    # Accept CLIENT SETNAME / SETINFO so stock clients connect cleanly.
    return OK


@command("COMMAND", -1)
def cmd_command(ctx, args):
    # redis-cli calls COMMAND DOCS at startup; an empty reply is acceptable.
    return []


# ------------------------------------------------------------------ strings
@command("GET", 2)
def cmd_get(ctx, args):
    return get_string(ctx, args[0])


@command("SET", -3, write=True)
def cmd_set(ctx, args):
    key, value, opts = args[0], args[1], [a.upper() for a in args[2:]]
    deadline = None
    nx = xx = keep_ttl = get = False
    i = 0
    while i < len(opts):
        opt = opts[i]
        if opt in (b"EX", b"PX", b"EXAT", b"PXAT") and deadline is None and i + 1 < len(opts):
            n = parse_int(args[2 + i + 1])
            if n <= 0:
                raise CommandError(Error("ERR invalid expire time in 'set' command"))
            now = ctx.store.now_ms()
            deadline = {b"EX": now + n * 1000, b"PX": now + n, b"EXAT": n * 1000, b"PXAT": n}[opt]
            i += 2
            continue
        if opt == b"NX" and not xx:
            nx = True
        elif opt == b"XX" and not nx:
            xx = True
        elif opt == b"KEEPTTL" and deadline is None:
            keep_ttl = True
        elif opt == b"GET":
            get = True
        else:
            raise CommandError(SYNTAX)
        i += 1
    if keep_ttl and deadline is not None:
        raise CommandError(SYNTAX)

    old = get_string(ctx, key) if get else ctx.store.get(key)
    exists = old is not None
    if (nx and exists) or (xx and not exists):
        ctx.custom_propagation = True  # condition failed: nothing changed, nothing to log
        return old if get else None

    ctx.store.set(key, value, keep_ttl=keep_ttl)
    if deadline is not None:
        ctx.store.expire_at(key, deadline)

    ctx.propagate(b"SET", key, value, *([b"KEEPTTL"] if keep_ttl else []))
    if deadline is not None:
        ctx.propagate(b"PEXPIREAT", key, b(deadline))
    return old if get else OK


@command("GETDEL", 2, write=True)
def cmd_getdel(ctx, args):
    value = get_string(ctx, args[0])
    if value is not None:
        ctx.store.delete(args[0])
    else:
        ctx.custom_propagation = True  # nothing changed, nothing to log
    return value


@command("MGET", -2)
def cmd_mget(ctx, args):
    out = []
    for key in args:
        value = ctx.store.get(key)
        out.append(value if isinstance(value, bytes) else None)
    return out


@command("MSET", -3, write=True)
def cmd_mset(ctx, args):
    if len(args) % 2:
        raise CommandError(Error("ERR wrong number of arguments for 'mset' command"))
    for i in range(0, len(args), 2):
        ctx.store.set(args[i], args[i + 1])
    return OK


@command("APPEND", 3, write=True)
def cmd_append(ctx, args):
    old = get_string(ctx, args[0]) or b""
    new = old + args[1]
    ctx.store.set(args[0], new, keep_ttl=True)
    return len(new)


@command("STRLEN", 2)
def cmd_strlen(ctx, args):
    return len(get_string(ctx, args[0]) or b"")


def _incr_by(ctx: Context, key: bytes, delta: int) -> int:
    old = get_string(ctx, key)
    current = parse_int(old) if old is not None else 0
    new = current + delta
    if not INT64_MIN <= new <= INT64_MAX:
        raise CommandError(Error("ERR increment or decrement would overflow"))
    ctx.store.set(key, b(new), keep_ttl=True)
    return new


@command("INCR", 2, write=True)
def cmd_incr(ctx, args):
    return _incr_by(ctx, args[0], 1)


@command("DECR", 2, write=True)
def cmd_decr(ctx, args):
    return _incr_by(ctx, args[0], -1)


@command("INCRBY", 3, write=True)
def cmd_incrby(ctx, args):
    return _incr_by(ctx, args[0], parse_int(args[1]))


@command("DECRBY", 3, write=True)
def cmd_decrby(ctx, args):
    return _incr_by(ctx, args[0], -parse_int(args[1]))


# --------------------------------------------------------------------- keys
@command("DEL", -2, write=True)
def cmd_del(ctx, args):
    return sum(ctx.store.delete(k) for k in args)


@command("EXISTS", -2)
def cmd_exists(ctx, args):
    return sum(k in ctx.store for k in args)


@command("TYPE", 2)
def cmd_type(ctx, args):
    value = ctx.store.get(args[0])
    kind = {bytes: "string", deque: "list", dict: "hash"}.get(type(value), "none")
    return SimpleString(kind)


@command("KEYS", 2)
def cmd_keys(ctx, args):
    return ctx.store.keys(args[0])


@command("DBSIZE", 1)
def cmd_dbsize(ctx, args):
    return len(ctx.store)


@command("FLUSHALL", -1, write=True)
def cmd_flushall(ctx, args):
    ctx.store.flush()
    return OK


@command("FLUSHDB", -1, write=True)
def cmd_flushdb(ctx, args):
    ctx.store.flush()
    return OK


def _expire(ctx: Context, key: bytes, deadline: int) -> int:
    ctx.custom_propagation = True
    if not ctx.store.expire_at(key, deadline):
        return 0
    if ctx.store.get(key) is None:  # deadline already passed: key was deleted
        ctx.propagate(b"DEL", key)
    else:
        ctx.propagate(b"PEXPIREAT", key, b(deadline))
    return 1


@command("EXPIRE", 3, write=True)
def cmd_expire(ctx, args):
    return _expire(ctx, args[0], ctx.store.now_ms() + parse_int(args[1]) * 1000)


@command("PEXPIRE", 3, write=True)
def cmd_pexpire(ctx, args):
    return _expire(ctx, args[0], ctx.store.now_ms() + parse_int(args[1]))


@command("EXPIREAT", 3, write=True)
def cmd_expireat(ctx, args):
    return _expire(ctx, args[0], parse_int(args[1]) * 1000)


@command("PEXPIREAT", 3, write=True)
def cmd_pexpireat(ctx, args):
    return _expire(ctx, args[0], parse_int(args[1]))


@command("PERSIST", 2, write=True)
def cmd_persist(ctx, args):
    return int(ctx.store.persist(args[0]))


@command("TTL", 2)
def cmd_ttl(ctx, args):
    ms = ctx.store.pttl(args[0])
    return ms if ms < 0 else (ms + 500) // 1000


@command("PTTL", 2)
def cmd_pttl(ctx, args):
    return ctx.store.pttl(args[0])


# -------------------------------------------------------------------- lists
@command("LPUSH", -3, write=True)
def cmd_lpush(ctx, args):
    lst = get_list(ctx, args[0], create=True)
    lst.extendleft(args[1:])
    return len(lst)


@command("RPUSH", -3, write=True)
def cmd_rpush(ctx, args):
    lst = get_list(ctx, args[0], create=True)
    lst.extend(args[1:])
    return len(lst)


def _pop(ctx: Context, args: list, left: bool):
    if len(args) > 2:
        raise CommandError(SYNTAX)
    count = parse_int(args[1]) if len(args) == 2 else None
    if count is not None and count < 0:
        raise CommandError(Error("ERR value is out of range, must be positive"))
    lst = get_list(ctx, args[0])
    if lst is None:
        return None
    pop = lst.popleft if left else lst.pop
    items = [pop() for _ in range(min(count if count is not None else 1, len(lst)))]
    if not lst:
        ctx.store.delete(args[0])
    return items if count is not None else items[0]


@command("LPOP", -2, write=True)
def cmd_lpop(ctx, args):
    return _pop(ctx, args, left=True)


@command("RPOP", -2, write=True)
def cmd_rpop(ctx, args):
    return _pop(ctx, args, left=False)


@command("LLEN", 2)
def cmd_llen(ctx, args):
    lst = get_list(ctx, args[0])
    return len(lst) if lst is not None else 0


@command("LRANGE", 4)
def cmd_lrange(ctx, args):
    lst = get_list(ctx, args[0])
    if lst is None:
        return []
    n = len(lst)
    start, stop = parse_int(args[1]), parse_int(args[2])
    if start < 0:
        start = max(0, n + start)
    if stop < 0:
        stop = n + stop
    stop = min(stop, n - 1)
    if start > stop:
        return []
    return [lst[i] for i in range(start, stop + 1)]


# ------------------------------------------------------------------- hashes
@command("HSET", -4, write=True)
def cmd_hset(ctx, args):
    if len(args) % 2 == 0:
        raise CommandError(Error("ERR wrong number of arguments for 'hset' command"))
    h = get_hash(ctx, args[0], create=True)
    added = 0
    for i in range(1, len(args), 2):
        added += args[i] not in h
        h[args[i]] = args[i + 1]
    return added


@command("HGET", 3)
def cmd_hget(ctx, args):
    h = get_hash(ctx, args[0])
    return h.get(args[1]) if h is not None else None


@command("HDEL", -3, write=True)
def cmd_hdel(ctx, args):
    h = get_hash(ctx, args[0])
    if h is None:
        return 0
    removed = sum(h.pop(f, None) is not None for f in args[1:])
    if not h:
        ctx.store.delete(args[0])
    return removed


@command("HGETALL", 2)
def cmd_hgetall(ctx, args):
    h = get_hash(ctx, args[0])
    if h is None:
        return []
    return Map(h)


@command("HLEN", 2)
def cmd_hlen(ctx, args):
    h = get_hash(ctx, args[0])
    return len(h) if h is not None else 0


@command("HEXISTS", 3)
def cmd_hexists(ctx, args):
    h = get_hash(ctx, args[0])
    return int(h is not None and args[1] in h)


@command("HINCRBY", 4, write=True)
def cmd_hincrby(ctx, args):
    delta = parse_int(args[2])  # validate before creating the hash
    h = get_hash(ctx, args[0], create=True)
    old = h.get(args[1])
    try:
        current = parse_int(old) if old is not None else 0
    except CommandError:
        raise CommandError(Error("ERR hash value is not an integer")) from None
    new = current + delta
    if not INT64_MIN <= new <= INT64_MAX:
        raise CommandError(Error("ERR increment or decrement would overflow"))
    h[args[1]] = b(new)
    return new


# ----------------------------------------------------------------- dispatch
def execute(ctx: Context, argv: list) -> object:
    """Run one command. Fills ctx.propagated for write commands that succeed."""
    ctx.propagated = []
    ctx.custom_propagation = False
    if not argv:
        return None
    name = argv[0].lower() if isinstance(argv[0], bytes) else b""
    cmd = COMMANDS.get(name)
    if cmd is None:
        shown = argv[0][:64].decode(errors="replace") if isinstance(argv[0], bytes) else "?"
        return Error(f"ERR unknown command '{shown}'")
    n = len(argv)
    if (cmd.arity > 0 and n != cmd.arity) or (cmd.arity < 0 and n < -cmd.arity):
        return Error(f"ERR wrong number of arguments for '{cmd.name.lower()}' command")
    if not all(isinstance(a, bytes) for a in argv):
        return Error("ERR Protocol error: expected bulk strings")

    try:
        reply = cmd.handler(ctx, argv[1:])
    except CommandError as e:
        ctx.propagated = []
        return e.reply
    if cmd.write and not ctx.custom_propagation:
        ctx.propagated.append(argv)
    return reply
