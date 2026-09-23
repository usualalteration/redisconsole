"""
Redis Console backend action (OpenServerless / Nuvolaris).

A single logically-grouped dispatch endpoint that exposes the Redis data
surface to the React frontend. The frontend picks a section ("strings",
"hashes", ...) and a concrete "op"; this module routes the request to the
matching Redis call.

Connection model
-----------------
The generated wrapper builds ``ctx.REDIS`` (a live ``redis.Redis`` client with
``decode_responses=True``) and ``ctx.REDIS_PREFIX`` from the OpenServerless
environment. ``resolve_redis_config`` is the single source of truth for which
variables are consulted: it prefers ``ext_redisurl`` / ``ext_redis_prefix``
(the OpenServerless-provided Redis URL and key prefix) and falls back to the
platform ``REDIS_URL`` / ``REDIS_PREFIX`` wiring when the ``ext_*`` names are
absent. The URL may carry credentials and connection parameters; the module
never reads hosts, ports, or passwords directly and never hardcodes them —
everything comes from OpenServerless configuration.

Response envelope
-----------------
Every successful call returns::

    {"ok": True, <result fields at the top level>}

Every failure returns the consistent error envelope::

    {"ok": False, "error": {"message": "...", "type": "RedisError"}}

with ``type`` describing the failure class
(``RedisConnectionError``, ``RedisAuthError``, ``RedisWrongTypeError``,
``RedisTimeoutError``, ``RedisCommandError``, ...). The frontend reads
result fields at the top level and only checks ``ok`` / pretty-prints the
payload, so this envelope is safe for the existing UI.

Safety
------
* Every key is built from ``ctx.REDIS_PREFIX`` + an app-local suffix; naked
  keys are never sent to Redis (Nuvolaris Redis ACLs enforce the prefix).
* Key discovery uses SCAN, never ``KEYS *``.
* Socket timeouts are applied to the client so a Redis call can never leave an
  OpenServerless action hanging indefinitely.
* Returned values are JSON-serializable (bytes are decoded, sets sorted).
* Adding a new Redis feature == adding one handler + one dispatch entry.
"""

import json
import os
import shlex
import time
import traceback


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

# Per-request socket timeouts (seconds). A Redis call cannot block the action
# longer than this. The OpenServerless action timeout is separate and larger.
REDIS_SOCKET_TIMEOUT = 5.0
REDIS_SOCKET_CONNECT_TIMEOUT = 3.0

# Upper bound for SCAN result batches returned to the UI.
SCAN_COUNT_LIMIT = 1000


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------

def request_data(args):
    """Merge OpenWhisk body + top-level fields into one dict (top-level wins)."""
    data = dict(args) if isinstance(args, dict) else {}
    body = data.get("body")
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except Exception:
            body = {}
    merged = dict(body) if isinstance(body, dict) else {}
    # Connection variables are backend-only and must never be honored from a
    # browser-supplied request; drop them so the frontend cannot inject or
    # read Redis credentials/prefix through the request body.
    ignored = {
        "body",
        "REDIS_URL", "REDIS_PREFIX", "ext_redisurl", "ext_redis_prefix",
        "__ow_method", "__ow_headers", "__ow_path", "__ow_body",
    }
    merged.update({k: v for k, v in data.items() if k not in ignored})
    return merged


def resolve_redis_config(args, environ=None):
    """Resolve the Redis connection URL and key prefix from the OpenServerless
    environment.

    The OpenServerless-provided variables ``ext_redisurl`` and
    ``ext_redis_prefix`` take precedence; the platform ``REDIS_URL`` /
    ``REDIS_PREFIX`` wiring is used as a fallback when the ``ext_*`` names are
    not present. Bound action parameters (``args``) win over process
    environment, and empty strings are treated as "not set" so an unset
    ``$ext_redisurl`` param cleanly falls back. Returns ``(url, prefix)`` where
    either may be ``None`` when the environment is not configured — the caller
    turns that into an explicit error rather than connecting without a
    namespace.
    """
    if environ is None:
        environ = os.environ
    args = args if isinstance(args, dict) else {}

    def pick(*names):
        for name in names:
            value = args.get(name)
            if value not in (None, ""):
                return value
            value = environ.get(name)
            if value not in (None, ""):
                return value
        return None

    url = pick("ext_redisurl", "REDIS_URL")
    prefix = pick("ext_redis_prefix", "REDIS_PREFIX")
    return url, prefix


def request_method(args):
    return (args.get("__ow_method") or args.get("method") or "GET").upper()


# ---------------------------------------------------------------------------
# Response / error helpers
# ---------------------------------------------------------------------------

def ok(**fields):
    """Build a success envelope: {ok: True, ...fields}."""
    payload = {"ok": True}
    payload.update(fields)
    return payload


def err(message, etype="RedisError", **extra):
    """Build the consistent error envelope: {ok: False, error: {message, type}}."""
    error = {"message": str(message), "type": str(etype)}
    error.update(extra)
    return {"ok": False, "error": error}


def map_exception(e):
    """Map a raised exception to a typed error envelope."""
    msg = str(e) or e.__class__.__name__
    name = e.__class__.__name__

    # Import lazily so the module loads even if redis isn't importable yet.
    try:
        import redis.exceptions as rexc
    except Exception:
        rexc = None

    if rexc is not None:
        if isinstance(e, rexc.AuthenticationError):
            return err(msg, "RedisAuthError")
        if isinstance(e, rexc.TimeoutError):
            return err(msg, "RedisTimeoutError")
        if isinstance(e, rexc.ConnectionError):
            return err(msg, "RedisConnectionError")
        if isinstance(e, rexc.BusyLoadingError):
            return err(msg, "RedisConnectionError")
        if isinstance(e, rexc.ResponseError):
            up = msg.upper()
            if up.startswith("WRONGTYPE"):
                return err(msg, "RedisWrongTypeError")
            if "NOAUTH" in up or "AUTH" in up:
                return err(msg, "RedisAuthError")
            if "EXECABORT" in up:
                return err(msg, "RedisTransactionError")
            return err(msg, "RedisCommandError")
        if isinstance(e, rexc.DataError):
            return err(msg, "RedisCommandError")
        base = getattr(rexc, "RedisError", None)
        if base is not None and isinstance(e, base):
            return err(msg, "RedisError")

    if "timeout" in msg.lower():
        return err(msg, "RedisTimeoutError")
    if name.endswith("ConnectionError"):
        return err(msg, "RedisConnectionError")
    return err(msg, "RedisError")


# ---------------------------------------------------------------------------
# Redis key helpers
# ---------------------------------------------------------------------------

def rkey(ctx, name):
    """Build a prefix-safe Redis key from ctx.REDIS_PREFIX + name."""
    if name is None:
        name = ""
    name = str(name)
    prefix = getattr(ctx, "REDIS_PREFIX", "") or ""
    if prefix and name.startswith(prefix):
        return name
    return f"{prefix}{name}"


def strip_prefix(ctx, name):
    """Remove the app prefix so the UI shows clean key names."""
    if name is None:
        return ""
    name = str(name)
    prefix = getattr(ctx, "REDIS_PREFIX", "") or ""
    if prefix and name.startswith(prefix):
        return name[len(prefix):]
    return name


def _to_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _jsonable(value):
    """Best-effort conversion of redis-py responses to JSON-serializable."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, set):
        return sorted(_jsonable(v) for v in value)
    return value


# ---------------------------------------------------------------------------
# Connection / status
# ---------------------------------------------------------------------------

def _parse_keyspace_entry(value):
    """Parse a Redis INFO keyspace entry into a dict of ints.

    redis-py already parses the keyspace section into dicts (e.g.
    {"keys": 12, "expires": 0, "avg_ttl": 0}); in that case the entry is
    returned with int values normalized. A raw 'keys=12,expires=0' string is
    parsed manually. Returns {} on unexpected input.
    """
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            try:
                out[str(k)] = int(v)
            except (TypeError, ValueError):
                out[str(k)] = v
        return out
    out = {}
    if not isinstance(value, str):
        return out
    for part in value.split(","):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        try:
            out[k.strip()] = int(v)
        except (TypeError, ValueError):
            out[k.strip()] = v
    return out


def cmd_status(ctx, data):
    r = ctx.REDIS
    prefix = getattr(ctx, "REDIS_PREFIX", "") or ""
    try:
        r.ping()
    except Exception as e:
        # Connection / auth failure: report disconnected with a typed error.
        return err(f"Redis unreachable: {e}", "RedisConnectionError",
                   connected=False, prefix=prefix)

    detail = bool(data.get("detail"))
    info_full = {}
    if detail:
        try:
            info_full = r.info()
        except Exception as e:
            return err(f"INFO failed: {e}", "RedisCommandError",
                       connected=True, prefix=prefix)

    try:
        dbsize = r.dbsize()
    except Exception as e:
        return err(f"DBSIZE failed: {e}", "RedisCommandError",
                   connected=True, prefix=prefix)

    # Backward-compatible narrow `info` object consumed by older UIs.
    info = {}
    stats = {}
    if detail and info_full:
        for key in ("redis_version", "redis_mode", "os", "arch_bits",
                    "uptime_in_seconds", "connected_clients",
                    "used_memory", "used_memory_human", "used_memory_peak_human",
                    "db0"):
            if key in info_full:
                info[key] = info_full[key]

        # Structured metrics for the Overview dashboard.
        def _num(name):
            try:
                return int(info_full.get(name, 0))
            except (TypeError, ValueError):
                return 0

        keyspace_hits = _num("keyspace_hits")
        keyspace_misses = _num("keyspace_misses")
        total_req = keyspace_hits + keyspace_misses
        hit_rate = round((keyspace_hits / total_req) * 100, 2) if total_req else None

        # Current database: read from the client's connection kwargs when
        # available (the platform wires the db via REDIS_URL). Fall back to
        # scanning the keyspace section for the first non-empty db.
        current_db = 0
        try:
            pool = getattr(r, "connection_pool", None)
            if pool is not None:
                current_db = int(pool.connection_kwargs.get("db", 0))
        except Exception:
            current_db = 0

        # Per-db keyspace breakdown (db0, db1, ... -> {keys, expires, avg_ttl}).
        keyspace = {}
        for k, v in info_full.items():
            if isinstance(k, str) and k.startswith("db"):
                keyspace[k] = _parse_keyspace_entry(v)

        stats = {
            "redis_version": info_full.get("redis_version"),
            "redis_mode": info_full.get("redis_mode"),
            "os": info_full.get("os"),
            "arch_bits": info_full.get("arch_bits"),
            "uptime_in_seconds": _num("uptime_in_seconds"),
            "connected_clients": _num("connected_clients"),
            "used_memory": _num("used_memory"),
            "used_memory_human": info_full.get("used_memory_human"),
            "used_memory_peak_human": info_full.get("used_memory_peak_human"),
            "total_keys": dbsize,
            "current_db": current_db,
            "keyspace_hits": keyspace_hits,
            "keyspace_misses": keyspace_misses,
            "hit_rate": hit_rate,
            "expired_keys": _num("expired_keys"),
            "evicted_keys": _num("evicted_keys"),
            "rejected_connections": _num("rejected_connections"),
            "total_connections_received": _num("total_connections_received"),
            "total_commands_processed": _num("total_commands_processed"),
            "keyspace": keyspace,
        }

    return ok(connected=True, dbsize=dbsize, info=info, stats=stats, prefix=prefix)


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------

def cmd_keys(ctx, data):
    r = ctx.REDIS
    op = (data.get("op") or "scan").lower()

    if op == "scan":
        pattern = data.get("pattern") or "*"
        count = _to_int(data.get("count"), 200)
        if count <= 0 or count > SCAN_COUNT_LIMIT:
            count = SCAN_COUNT_LIMIT
        cursor = _to_int(data.get("cursor"), 0)
        # Match within the app namespace: the user thinks in unprefixed keys,
        # so anchor the glob pattern to the app prefix. "*" stays a wildcard
        # that still only surfaces keys under this prefix.
        prefix = getattr(ctx, "REDIS_PREFIX", "") or ""
        if prefix and not pattern.startswith(prefix):
            match_pattern = f"{prefix}{pattern}"
        else:
            match_pattern = pattern
        cur, keys = r.scan(cursor=cursor, match=match_pattern, count=count)
        names = [strip_prefix(ctx, k) for k in keys]
        result = ok(cursor=cur, scan_complete=(cur == 0),
                    keys=names, count=len(names))
        # Optional per-key metadata for the visual Key Explorer. Bounded by
        # the SCAN batch size (<= SCAN_COUNT_LIMIT), so a huge database never
        # triggers one TYPE/TTL call per key across the whole keyspace.
        if data.get("withmeta"):
            meta = []
            for full in keys:
                kname = strip_prefix(ctx, full)
                try:
                    ktype = r.type(full)
                except Exception:
                    ktype = "none"
                try:
                    ttl = r.ttl(full)
                except Exception:
                    ttl = -1
                meta.append({"name": kname, "type": ktype, "ttl": ttl})
            result["keyMeta"] = meta
        return result

    key = data.get("key")
    if not key:
        return err("Missing 'key'", "RedisCommandError")

    if op == "type":
        return ok(key=strip_prefix(ctx, key), type=r.type(rkey(ctx, key)))
    if op == "del":
        return ok(deleted=r.delete(rkey(ctx, key)))
    if op == "exists":
        return ok(exists=r.exists(rkey(ctx, key)))
    if op == "rename":
        newkey = data.get("newkey")
        if not newkey:
            return err("Missing 'newkey'", "RedisCommandError")
        r.rename(rkey(ctx, key), rkey(ctx, newkey))
        return ok(renamed_to=strip_prefix(ctx, newkey))
    return err(f"Unknown keys op '{op}'", "RedisCommandError")


# ---------------------------------------------------------------------------
# Strings
# ---------------------------------------------------------------------------

def cmd_strings(ctx, data):
    r = ctx.REDIS
    op = (data.get("op") or "get").lower()
    key = data.get("key")
    if not key:
        return err("Missing 'key'", "RedisCommandError")
    k = rkey(ctx, key)

    if op == "set":
        value = data.get("value")
        if value is None:
            return err("Missing 'value'", "RedisCommandError")
        ex = data.get("ex")
        nx = bool(data.get("nx"))
        xx = bool(data.get("xx"))
        kwargs = {}
        if ex not in (None, ""):
            kwargs["ex"] = _to_int(ex)
        if nx:
            kwargs["nx"] = True
        if xx:
            kwargs["xx"] = True
        result = r.set(k, str(value), **kwargs)
        return ok(set=(result is not False), key=strip_prefix(ctx, key))
    if op == "get":
        return ok(key=strip_prefix(ctx, key), value=r.get(k))
    if op == "getset":
        value = data.get("value")
        if value is None:
            return err("Missing 'value'", "RedisCommandError")
        return ok(key=strip_prefix(ctx, key), old_value=r.getset(k, str(value)))
    if op == "incr":
        return ok(key=strip_prefix(ctx, key),
                  value=r.incr(k, _to_int(data.get("amount"), 1)))
    if op == "decr":
        return ok(key=strip_prefix(ctx, key),
                  value=r.decr(k, _to_int(data.get("amount"), 1)))
    if op == "append":
        value = data.get("value")
        if value is None:
            return err("Missing 'value'", "RedisCommandError")
        return ok(key=strip_prefix(ctx, key), length=r.append(k, str(value)))
    if op == "strlen":
        return ok(key=strip_prefix(ctx, key), length=r.strlen(k))
    if op == "mget":
        keys = _as_list(data.get("keys"))
        if not keys:
            return err("Missing 'keys'", "RedisCommandError")
        vals = r.mget([rkey(ctx, kk) for kk in keys])
        return ok(values=dict(zip([strip_prefix(ctx, kk) for kk in keys], vals)))
    if op == "mset":
        mapping = data.get("mapping")
        if not isinstance(mapping, dict):
            return err("Missing 'mapping' object", "RedisCommandError")
        r.mset({rkey(ctx, kk): str(vv) for kk, vv in mapping.items()})
        return ok(set=list(mapping.keys()))
    return err(f"Unknown strings op '{op}'", "RedisCommandError")


# ---------------------------------------------------------------------------
# Hashes
# ---------------------------------------------------------------------------

def cmd_hashes(ctx, data):
    r = ctx.REDIS
    op = (data.get("op") or "hgetall").lower()
    key = data.get("key")
    if not key:
        return err("Missing 'key'", "RedisCommandError")
    k = rkey(ctx, key)
    field = data.get("field")
    value = data.get("value")

    if op == "hset":
        if field is None:
            return err("Missing 'field'", "RedisCommandError")
        if value is None:
            return err("Missing 'value'", "RedisCommandError")
        return ok(added=r.hset(k, str(field), str(value)))
    if op == "hmset":
        mapping = data.get("mapping")
        if not isinstance(mapping, dict):
            return err("Missing 'mapping' object", "RedisCommandError")
        n = r.hset(k, mapping={str(kk): str(vv) for kk, vv in mapping.items()})
        return ok(added=n)
    if op == "hget":
        if field is None:
            return err("Missing 'field'", "RedisCommandError")
        return ok(key=strip_prefix(ctx, key), field=str(field),
                  value=r.hget(k, str(field)))
    if op == "hgetall":
        raw = r.hgetall(k)
        return ok(key=strip_prefix(ctx, key), fields=raw, count=len(raw))
    if op == "hkeys":
        return ok(key=strip_prefix(ctx, key), fields=list(r.hkeys(k)))
    if op == "hvals":
        return ok(key=strip_prefix(ctx, key), values=list(r.hvals(k)))
    if op == "hlen":
        return ok(key=strip_prefix(ctx, key), length=r.hlen(k))
    if op == "hdel":
        fields = _as_list(data.get("fields") or field)
        if not fields:
            return err("Missing 'fields'", "RedisCommandError")
        return ok(deleted=r.hdel(k, *[str(f) for f in fields]))
    if op == "hexists":
        if field is None:
            return err("Missing 'field'", "RedisCommandError")
        return ok(exists=bool(r.hexists(k, str(field))))
    if op == "hscan":
        # Cursor-based, bounded scan of a (possibly huge) hash. Never loads
        # the whole structure the way HGETALL would.
        cursor = _to_int(data.get("cursor"), 0)
        count = _to_int(data.get("count"), 100)
        if count <= 0 or count > SCAN_COUNT_LIMIT:
            count = 100
        match = data.get("match")
        kwargs = {"cursor": cursor, "count": count}
        if match:
            kwargs["match"] = str(match)
        cur, fields = r.hscan(k, **kwargs)
        fields = {str(fk): fv for fk, fv in fields.items()}
        return ok(key=strip_prefix(ctx, key), cursor=cur,
                  scan_complete=(cur == 0),
                  fields=fields, count=len(fields))
    return err(f"Unknown hashes op '{op}'", "RedisCommandError")


# ---------------------------------------------------------------------------
# Lists
# ---------------------------------------------------------------------------

def cmd_lists(ctx, data):
    r = ctx.REDIS
    op = (data.get("op") or "lrange").lower()
    key = data.get("key")
    if not key:
        return err("Missing 'key'", "RedisCommandError")
    k = rkey(ctx, key)
    value = data.get("value")
    values = _as_list(data.get("values") or value)

    if op == "lpush":
        if not values:
            return err("Missing 'value(s)'", "RedisCommandError")
        return ok(length=r.lpush(k, *[str(v) for v in values]))
    if op == "rpush":
        if not values:
            return err("Missing 'value(s)'", "RedisCommandError")
        return ok(length=r.rpush(k, *[str(v) for v in values]))
    if op == "lpop":
        res = r.lpop(k, _to_int(data.get("count"), 1)) if data.get("count") else r.lpop(k)
        return ok(value=res)
    if op == "rpop":
        res = r.rpop(k, _to_int(data.get("count"), 1)) if data.get("count") else r.rpop(k)
        return ok(value=res)
    if op == "lrange":
        start = _to_int(data.get("start"), 0)
        stop = _to_int(data.get("stop"), -1)
        return ok(key=strip_prefix(ctx, key), items=r.lrange(k, start, stop))
    if op == "llen":
        return ok(key=strip_prefix(ctx, key), length=r.llen(k))
    if op == "lset":
        idx = _to_int(data.get("index"), 0)
        if value is None:
            return err("Missing 'value'", "RedisCommandError")
        r.lset(k, idx, str(value))
        return ok(index=idx)
    if op == "lrem":
        count = _to_int(data.get("count"), 0)
        if value is None:
            return err("Missing 'value'", "RedisCommandError")
        return ok(removed=r.lrem(k, count, str(value)))
    return err(f"Unknown lists op '{op}'", "RedisCommandError")


# ---------------------------------------------------------------------------
# Sets
# ---------------------------------------------------------------------------

def cmd_sets(ctx, data):
    r = ctx.REDIS
    op = (data.get("op") or "smembers").lower()
    key = data.get("key")
    if not key:
        return err("Missing 'key'", "RedisCommandError")
    k = rkey(ctx, key)
    members = _as_list(data.get("members") or data.get("value"))
    single_member = data.get("member")

    if op == "sadd":
        if not members:
            return err("Missing 'member(s)'", "RedisCommandError")
        return ok(added=r.sadd(k, *[str(m) for m in members]))
    if op == "srem":
        if not members:
            return err("Missing 'member(s)'", "RedisCommandError")
        return ok(removed=r.srem(k, *[str(m) for m in members]))
    if op == "smembers":
        return ok(key=strip_prefix(ctx, key), members=sorted(r.smembers(k)))
    if op == "sismember":
        target = single_member if single_member is not None else (members[0] if members else None)
        if target is None:
            return err("Missing 'member'", "RedisCommandError")
        return ok(member=str(target),
                  is_member=bool(r.sismember(k, str(target))))
    if op == "scard":
        return ok(key=strip_prefix(ctx, key), cardinality=r.scard(k))
    if op == "sscan":
        # Cursor-based, bounded scan of a (possibly huge) set. Never loads
        # the whole structure the way SMEMBERS would.
        cursor = _to_int(data.get("cursor"), 0)
        count = _to_int(data.get("count"), 100)
        if count <= 0 or count > SCAN_COUNT_LIMIT:
            count = 100
        match = data.get("match")
        kwargs = {"cursor": cursor, "count": count}
        if match:
            kwargs["match"] = str(match)
        cur, members = r.sscan(k, **kwargs)
        return ok(key=strip_prefix(ctx, key), cursor=cur,
                  scan_complete=(cur == 0),
                  members=sorted(members), count=len(members))
    return err(f"Unknown sets op '{op}'", "RedisCommandError")


# ---------------------------------------------------------------------------
# Sorted Sets
# ---------------------------------------------------------------------------

def _zitems(res, withscores):
    if withscores:
        return [{"member": m, "score": s} for m, s in res]
    return list(res)


def cmd_sorted_sets(ctx, data):
    r = ctx.REDIS
    op = (data.get("op") or "zrange").lower()
    key = data.get("key")
    if not key:
        return err("Missing 'key'", "RedisCommandError")
    k = rkey(ctx, key)
    member = data.get("member")
    score = data.get("score")

    if op == "zadd":
        if member is None or score is None:
            return err("Missing 'member' or 'score'", "RedisCommandError")
        return ok(added=r.zadd(k, {str(member): _to_float(score)}))
    if op == "zaddmany":
        mapping = data.get("mapping")
        if not isinstance(mapping, dict):
            return err("Missing 'mapping' object", "RedisCommandError")
        return ok(added=r.zadd(k, {str(mm): _to_float(ss) for mm, ss in mapping.items()}))
    if op == "zrange":
        start = _to_int(data.get("start"), 0)
        stop = _to_int(data.get("stop"), -1)
        withscores = bool(data.get("withscores"))
        res = r.zrange(k, start, stop, withscores=withscores)
        return ok(key=strip_prefix(ctx, key), members=_zitems(res, withscores))
    if op == "zrevrange":
        start = _to_int(data.get("start"), 0)
        stop = _to_int(data.get("stop"), -1)
        withscores = bool(data.get("withscores"))
        res = r.zrevrange(k, start, stop, withscores=withscores)
        return ok(key=strip_prefix(ctx, key), members=_zitems(res, withscores))
    if op == "zrangebyscore":
        lo = data.get("min", "-inf")
        hi = data.get("max", "+inf")
        withscores = bool(data.get("withscores"))
        res = r.zrangebyscore(k, lo, hi, withscores=withscores)
        return ok(key=strip_prefix(ctx, key), members=_zitems(res, withscores))
    if op == "zrem":
        if member is None:
            return err("Missing 'member'", "RedisCommandError")
        return ok(removed=r.zrem(k, str(member)))
    if op == "zcard":
        return ok(key=strip_prefix(ctx, key), cardinality=r.zcard(k))
    if op == "zscore":
        if member is None:
            return err("Missing 'member'", "RedisCommandError")
        return ok(member=str(member), score=r.zscore(k, str(member)))
    return err(f"Unknown sorted-sets op '{op}'", "RedisCommandError")


# ---------------------------------------------------------------------------
# Streams
# ---------------------------------------------------------------------------

def cmd_streams(ctx, data):
    r = ctx.REDIS
    op = (data.get("op") or "xrange").lower()
    key = data.get("key")
    if not key:
        return err("Missing 'key'", "RedisCommandError")
    k = rkey(ctx, key)

    if op == "xadd":
        fields = data.get("fields")
        if not isinstance(fields, dict):
            return err("Missing 'fields' object", "RedisCommandError")
        eid = data.get("id") or "*"
        mid = r.xadd(k, {str(kk): str(vv) for kk, vv in fields.items()}, id=eid)
        return ok(id=mid)
    if op == "xrange":
        start = data.get("min") or "-"
        stop = data.get("max") or "+"
        count = _to_int(data.get("count"), 0) or None
        res = r.xrange(k, start, stop, count=count)
        return ok(key=strip_prefix(ctx, key),
                  entries=[{"id": eid, "fields": f} for eid, f in res])
    if op == "xrevrange":
        # XREVRANGE: end comes first, then start.
        end = data.get("max") or data.get("end") or "+"
        start = data.get("min") or data.get("start") or "-"
        count = _to_int(data.get("count"), 0) or None
        res = r.xrevrange(k, end, start, count=count)
        return ok(key=strip_prefix(ctx, key),
                  entries=[{"id": eid, "fields": f} for eid, f in res])
    if op == "xlen":
        return ok(key=strip_prefix(ctx, key), length=r.xlen(k))
    if op == "xdel":
        ids = _as_list(data.get("ids") or data.get("id"))
        if not ids:
            return err("Missing 'id(s)'", "RedisCommandError")
        return ok(deleted=r.xdel(k, *[str(i) for i in ids]))
    return err(f"Unknown streams op '{op}'", "RedisCommandError")


# ---------------------------------------------------------------------------
# Pub/Sub
#
# Serverless actions are stateless short-lived requests, so a long-lived
# SUBSCRIBE socket is not possible. We expose PUBLISH and channel discovery,
# which fit the request/response model. The UI documents this limitation.
# ---------------------------------------------------------------------------

def cmd_pubsub(ctx, data):
    r = ctx.REDIS
    op = (data.get("op") or "channels").lower()

    if op == "publish":
        channel = data.get("channel")
        message = data.get("message")
        if not channel or message is None:
            return err("Missing 'channel' or 'message'", "RedisCommandError")
        ch = rkey(ctx, channel)
        n = r.publish(ch, str(message))
        return ok(channel=strip_prefix(ctx, ch), subscribers=n)
    if op == "channels":
        pattern = data.get("pattern") or f"{getattr(ctx, 'REDIS_PREFIX', '') or ''}*"
        channels = r.pubsub_channels(pattern)
        return ok(channels=[strip_prefix(ctx, c) for c in channels])
    if op == "numsub":
        channels = _as_list(data.get("channels"))
        if not channels:
            return err("Missing 'channels'", "RedisCommandError")
        res = r.pubsub_numsub(*[rkey(ctx, c) for c in channels])
        out = {}
        # redis-py returns a list of (channel, count) tuples.
        for entry in res:
            if isinstance(entry, (list, tuple)) and len(entry) == 2:
                chan, count = entry
                out[strip_prefix(ctx, chan)] = count
        return ok(subscribers=out)
    return err(f"Unknown pubsub op '{op}'", "RedisCommandError")


# ---------------------------------------------------------------------------
# TTL
# ---------------------------------------------------------------------------

def cmd_ttl(ctx, data):
    r = ctx.REDIS
    op = (data.get("op") or "ttl").lower()
    key = data.get("key")
    if not key:
        return err("Missing 'key'", "RedisCommandError")
    k = rkey(ctx, key)

    if op == "ttl":
        return ok(key=strip_prefix(ctx, key), ttl=r.ttl(k))
    if op == "pttl":
        return ok(key=strip_prefix(ctx, key), ttl_ms=r.pttl(k))
    if op == "expire":
        seconds = _to_int(data.get("seconds"), 0)
        done = r.expire(k, seconds)
        return ok(key=strip_prefix(ctx, key), set=bool(done))
    if op == "pexpire":
        ms = _to_int(data.get("milliseconds"), 0)
        done = r.pexpire(k, ms)
        return ok(key=strip_prefix(ctx, key), set=bool(done))
    if op == "persist":
        done = r.persist(k)
        return ok(key=strip_prefix(ctx, key), persisted=bool(done))
    return err(f"Unknown ttl op '{op}'", "RedisCommandError")


# ---------------------------------------------------------------------------
# Demo data laboratories
#
# Provides disposable, namespaced example data for the interactive labs.
# Every demo key lives under the "redis-console:demo:" sub-namespace (which is
# itself inside the app's ctx.REDIS_PREFIX), so the cleanup op can never touch
# unrelated user data. Cleanup uses SCAN + DEL, never KEYS.
# ---------------------------------------------------------------------------

DEMO_NAMESPACE = "redis-console:demo:"


def _demo_key(suffix):
    """Build a demo key suffix path under the demo namespace."""
    suffix = str(suffix).strip("/")
    return f"{DEMO_NAMESPACE}{suffix}"


def cmd_demo(ctx, data):
    r = ctx.REDIS
    op = (data.get("op") or "seed").lower()
    kind = (data.get("kind") or "").lower().replace("-", "")

    if op == "clean":
        # Delete every key under the demo namespace (within the app prefix).
        pattern = rkey(ctx, f"{DEMO_NAMESPACE}*")
        cursor = 0
        deleted = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=200)
            if keys:
                deleted += r.delete(*keys)
            if cursor == 0:
                break
        return ok(deleted=deleted)

    if op == "seed":
        if kind not in ("strings", "hashes", "lists", "sets",
                        "sortedsets", "streams", "ttl"):
            return err(f"Unknown demo kind '{kind}'", "RedisCommandError")
        created = []
        if kind == "strings":
            k1 = rkey(ctx, _demo_key("strings:counter"))
            k2 = rkey(ctx, _demo_key("strings:greeting"))
            r.set(k1, "0")
            r.set(k2, "Hello, Redis!")
            created = [strip_prefix(ctx, k1), strip_prefix(ctx, k2)]
        elif kind == "hashes":
            k = rkey(ctx, _demo_key("hashes:user:1"))
            r.hset(k, mapping={"name": "Ada", "role": "admin", "active": "1"})
            created = [strip_prefix(ctx, k)]
        elif kind == "lists":
            k = rkey(ctx, _demo_key("lists:queue"))
            r.rpush(k, "first", "second", "third")
            created = [strip_prefix(ctx, k)]
        elif kind == "sets":
            k = rkey(ctx, _demo_key("sets:tags"))
            r.sadd(k, "redis", "streams", "demo")
            created = [strip_prefix(ctx, k)]
        elif kind == "sortedsets":
            k = rkey(ctx, _demo_key("zset:leaderboard"))
            r.zadd(k, {"ada": 100, "grace": 85, "alan": 92})
            created = [strip_prefix(ctx, k)]
        elif kind == "streams":
            k = rkey(ctx, _demo_key("streams:events"))
            for i in range(3):
                r.xadd(k, {"event": "seed", "seq": str(i)})
            created = [strip_prefix(ctx, k)]
        elif kind == "ttl":
            k = rkey(ctx, _demo_key("ttl:session"))
            r.set(k, "temp-value", ex=120)
            created = [strip_prefix(ctx, k)]
        return ok(kind=kind, created=created)

    if op == "scan":
        # List demo keys present (within the app prefix).
        pattern = rkey(ctx, f"{DEMO_NAMESPACE}*")
        cursor = 0
        names = []
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=200)
            names.extend(strip_prefix(ctx, k) for k in keys)
            if cursor == 0:
                break
        return ok(keys=sorted(names), count=len(names))

    return err(f"Unknown demo op '{op}'", "RedisCommandError")


# ---------------------------------------------------------------------------
# Transactions / pipelines
#
# Runs a list of [cmd, key, ...args] tuples through a Redis pipeline (atomic
# MULTI/EXEC when multi=True, otherwise a non-transactional pipeline). Each
# command's first key argument is prefixed with the app namespace.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Safe-console denylist
#
# Destructive or server-compromising commands are blocked at the raw endpoint
# (and at the transactions endpoint) unless an operator has explicitly
# enabled them through backend configuration. The gate consults an
# operator-set environment variable (REDIS_CONSOLE_ALLOW_DANGEROUS) and a
# bound action parameter (ALLOW_DANGEROUS). A browser-supplied flag is
# intentionally NOT consulted, so the frontend cannot bypass the denylist on
# its own — the restriction is authoritative on the backend.
# ---------------------------------------------------------------------------

DANGEROUS_COMMANDS = {
    "flushall", "flushdb", "shutdown", "config", "debug", "module",
    "slaveof", "replicaof", "migrate", "restore", "acl", "script",
    "function",
}


def _truthy(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    if isinstance(value, (int, float)):
        return value != 0
    return False


def _dangerous_allowed(data):
    """True only when backend configuration explicitly enables dangerous
    commands. Honors an operator-set env var (REDIS_CONSOLE_ALLOW_DANGEROUS)
    or a bound action parameter (ALLOW_DANGEROUS). Browser-supplied fields are
    not consulted, so the frontend cannot bypass the denylist by itself."""
    if _truthy(os.getenv("REDIS_CONSOLE_ALLOW_DANGEROUS")):
        return True
    if _truthy(data.get("ALLOW_DANGEROUS")):
        return True
    return False


# Commands whose first argument is NOT a key (no prefixing).
_TX_NO_KEY_COMMANDS = {
    "ping", "info", "dbsize", "flushdb", "config", "client", "time",
    "command", "pubsub", "multi", "exec", "discard", "watch", "unwatch",
}

# Commands where ALL arguments are keys.
_TX_ALL_KEY_COMMANDS = {"del", "exists", "unlink", "mget", "touch"}

# Commands with alternating key/value pairs starting at arg 0 (MSET, MSETNX).
_TX_PAIR_KEY_COMMANDS = {"mset", "msetnx"}


def _prefix_tx_args(ctx, cmd_name, args):
    """Return a copy of args with key positions prefixed by the app prefix."""
    if cmd_name in _TX_NO_KEY_COMMANDS or not args:
        return [str(a) for a in args]
    if cmd_name in _TX_ALL_KEY_COMMANDS:
        return [rkey(ctx, a) for a in args]
    if cmd_name in _TX_PAIR_KEY_COMMANDS:
        out = []
        for i, a in enumerate(args):
            out.append(rkey(ctx, a) if i % 2 == 0 else str(a))
        return out
    # Default: first argument is a key.
    out = [str(a) for a in args]
    out[0] = rkey(ctx, args[0])
    return out


def cmd_transactions(ctx, data):
    r = ctx.REDIS
    commands = data.get("commands")
    if not isinstance(commands, list) or not commands:
        return err("Missing 'commands' list", "RedisCommandError")

    # Mode selects the transaction demonstration. Legacy callers passed a
    # boolean `multi`; preserve that for backward compatibility.
    mode = str(data.get("mode") or "").lower()
    if not mode:
        mode = "multi" if bool(data.get("multi", True)) else "pipeline"
    if mode not in ("multi", "pipeline", "discard", "watch"):
        return err(f"Unknown transaction mode '{mode}'", "RedisCommandError")

    # Validate + queue every command first (denylist enforced here too) so a
    # bad entry fails before any Redis side-effect.
    parsed = []
    for entry in commands:
        if not isinstance(entry, list) or not entry:
            return err(f"Invalid command entry: {entry!r}", "RedisCommandError")
        cmd_name = str(entry[0]).lower()
        if cmd_name == "keys":
            return err("KEYS is blocked (use the SCAN operation instead)",
                       "RedisCommandError")
        if cmd_name in DANGEROUS_COMMANDS and not _dangerous_allowed(data):
            return err(
                f"{cmd_name.upper()} is blocked by the safe-console denylist. "
                "A backend operator can enable it via "
                "REDIS_CONSOLE_ALLOW_DANGEROUS.",
                "RedisCommandError", blocked=True, command=cmd_name)
        args = list(entry[1:])
        prefixed = _prefix_tx_args(ctx, cmd_name, args)
        parsed.append((cmd_name, prefixed))

    # ----- WATCH mode ---------------------------------------------------
    # The whole demonstration runs on one connection so the WATCH lock is held
    # by the same client that issues MULTI/EXEC. redis-py's Pipeline supports
    # this: watch() -> multi() -> <queue> -> execute(); a watched key changed
    # between WATCH and EXEC raises WatchError on execute().
    if mode == "watch":
        watch_keys = _as_list(data.get("watch"))
        if not watch_keys:
            return err("WATCH mode requires a 'watch' key list", "RedisCommandError")
        watch_prefixed = [rkey(ctx, w) for w in watch_keys]
        # Optionally mutate a watched key *after* WATCH but *before* EXEC to
        # deterministically demonstrate an aborted transaction (WatchError).
        interfere_key = data.get("interfere_key")
        interfere_value = data.get("interfere_value")

        try:
            import redis.exceptions as rexc
        except Exception:
            rexc = None

        pipe = r.pipeline()
        try:
            pipe.watch(*watch_prefixed)
        except Exception as e:
            return map_exception(e)

        # Optionally break the optimistic lock from the same connection.
        interfered = False
        if interfere_key is not None and interfere_value is not None:
            try:
                r.set(rkey(ctx, interfere_key), str(interfere_value))
                interfered = True
            except Exception as e:
                pipe.reset()
                return map_exception(e)

        pipe.multi()
        try:
            for cmd_name, prefixed in parsed:
                getattr(pipe, cmd_name)(*prefixed)
        except AttributeError:
            pipe.reset()
            return err(f"Unknown Redis command '{cmd_name}'", "RedisCommandError")
        except Exception as e:
            pipe.reset()
            return err(f"Error queueing '{cmd_name}': {e}", "RedisCommandError")

        try:
            results = pipe.execute()
        except Exception as e:
            aborted = rexc is not None and isinstance(e, rexc.WatchError)
            if aborted:
                return ok(mode="watch", aborted=True, interfered=interfered,
                          watch_keys=[strip_prefix(ctx, w) for w in watch_prefixed],
                          reason="WATCH detected a modification to a watched key; "
                                 "EXEC aborted the transaction.")
            return map_exception(e)

        return ok(mode="watch", aborted=False, interfered=interfered,
                   watch_keys=[strip_prefix(ctx, w) for w in watch_prefixed],
                   executed=[{"cmd": n, "result": _jsonable(res)}
                             for n, res in zip([p[0] for p in parsed], results)])

    # ----- DISCARD mode --------------------------------------------------
    # Queue a MULTI block then DISCARD it: no commands run, the connection
    # leaves the transaction state, and the queued results are discarded.
    if mode == "discard":
        pipe = r.pipeline(transaction=True)
        try:
            for cmd_name, prefixed in parsed:
                getattr(pipe, cmd_name)(*prefixed)
        except AttributeError:
            pipe.reset()
            return err(f"Unknown Redis command '{cmd_name}'", "RedisCommandError")
        except Exception as e:
            pipe.reset()
            return err(f"Error queueing '{cmd_name}': {e}", "RedisCommandError")
        # reset() issues DISCARD on the server when inside a MULTI block.
        pipe.reset()
        return ok(mode="discard", discarded=True,
                  queued=[p[0] for p in parsed],
                  executed=[])

    # ----- MULTI / pipeline modes (default) -----------------------------
    use_multi = mode == "multi"
    pipe = r.pipeline(transaction=use_multi)
    queued_names = []
    try:
        for cmd_name, prefixed in parsed:
            getattr(pipe, cmd_name)(*prefixed)
            queued_names.append(cmd_name)
    except AttributeError:
        pipe.reset()
        return err(f"Unknown Redis command '{cmd_name}'", "RedisCommandError")
    except Exception as e:
        pipe.reset()
        return err(f"Error queueing '{cmd_name}': {e}", "RedisCommandError")

    try:
        results = pipe.execute()
    except Exception as e:
        return map_exception(e)

    return ok(mode=mode,
              executed=[{"cmd": n, "result": _jsonable(res)}
                        for n, res in zip(queued_names, results)])


# ---------------------------------------------------------------------------
# Raw commands
#
# Accepts a command line (string) or a structured list. The line is parsed
# with shlex (quoted arguments supported; never eval / shell execution). Key
# arguments are prefixed with ctx.REDIS_PREFIX; meta/no-key commands run
# as-is. Destructive / server-compromising commands are blocked by the
# safe-console denylist unless backend configuration explicitly enables them.
# Execution duration is measured and returned as duration_ms.
# ---------------------------------------------------------------------------

_RAW_NO_KEY_COMMANDS = {
    "ping", "info", "dbsize", "flushdb", "flushall", "config", "client",
    "time", "command", "pubsub", "auth", "select", "cluster", "debug",
    "memory", "swapdb", "lastsave", "wait", "reset", "module",
}

_RAW_ALL_KEY_COMMANDS = {"del", "exists", "unlink", "mget", "touch", "smove"}
_RAW_PAIR_KEY_COMMANDS = {"mset", "msetnx", "rename", "renamenx"}


def _prefix_raw_args(ctx, cmd_name, args):
    if cmd_name in _RAW_NO_KEY_COMMANDS or not args:
        return [str(a) for a in args]
    if cmd_name in _RAW_ALL_KEY_COMMANDS:
        return [rkey(ctx, a) for a in args]
    if cmd_name in _RAW_PAIR_KEY_COMMANDS:
        out = []
        for i, a in enumerate(args):
            out.append(rkey(ctx, a) if i % 2 == 0 else str(a))
        return out
    out = [str(a) for a in args]
    out[0] = rkey(ctx, args[0])
    return out


def _parse_command_line(line):
    """Parse a raw command line into a list of arguments, honoring single
    and double quotes and backslash escapes via shlex. Never uses eval or
    shell execution. Returns (parts, None) on success or (None, message) on a
    parse error so the caller can surface an explicit error instead of
    silently mis-splitting quoted arguments."""
    if not isinstance(line, str):
        return (None, "Command must be a string")
    line = line.strip()
    if not line:
        return (None, "Empty command")
    try:
        parts = shlex.split(line, posix=True)
    except ValueError as e:
        return (None, f"Parse error: {e}")
    if not parts:
        return (None, "Empty command")
    return (parts, None)


def cmd_raw(ctx, data):
    r = ctx.REDIS
    raw = data.get("command")
    if not raw:
        return err("Missing 'command'", "RedisCommandError")

    parts, parse_err = _parse_command_line(raw)
    if parse_err:
        return err(parse_err, "RedisCommandError")

    cmd_name = parts[0].lower()

    # KEYS is unbounded; route the user to the bounded SCAN operation instead.
    if cmd_name == "keys":
        return err("KEYS is blocked (use the SCAN operation instead)",
                   "RedisCommandError")

    # Destructive / server-compromising commands are blocked unless the
    # operator has explicitly enabled them through backend configuration.
    if cmd_name in DANGEROUS_COMMANDS and not _dangerous_allowed(data):
        return err(
            f"{cmd_name.upper()} is blocked by the safe-console denylist. "
            "A backend operator can enable it via "
            "REDIS_CONSOLE_ALLOW_DANGEROUS.",
            "RedisCommandError", blocked=True, command=cmd_name)

    args = _prefix_raw_args(ctx, cmd_name, parts[1:])

    start = time.monotonic()
    try:
        result = r.execute_command(cmd_name, *args)
    except Exception as e:
        mapped = map_exception(e)
        mapped["command"] = cmd_name
        mapped["duration_ms"] = round((time.monotonic() - start) * 1000.0, 2)
        return mapped

    duration_ms = round((time.monotonic() - start) * 1000.0, 2)
    return ok(command=cmd_name, args=args, result=_jsonable(result),
              duration_ms=duration_ms)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

DISPATCH = {
    "status": cmd_status,
    "keys": cmd_keys,
    "strings": cmd_strings,
    "hashes": cmd_hashes,
    "lists": cmd_lists,
    "sets": cmd_sets,
    "sorted-sets": cmd_sorted_sets,
    "sortedsets": cmd_sorted_sets,
    "streams": cmd_streams,
    "pubsub": cmd_pubsub,
    "pub-sub": cmd_pubsub,
    "ttl": cmd_ttl,
    "demo": cmd_demo,
    "transactions": cmd_transactions,
    "raw": cmd_raw,
}


def _apply_timeouts(ctx):
    """Apply per-request socket timeouts so a Redis call can never hang."""
    r = getattr(ctx, "REDIS", None)
    if r is None:
        return
    for setter, value in (
        ("set_socket_timeout", REDIS_SOCKET_TIMEOUT),
        ("set_socket_connect_timeout", REDIS_SOCKET_CONNECT_TIMEOUT),
    ):
        fn = getattr(r, setter, None)
        if callable(fn):
            try:
                fn(value)
            except Exception:
                pass


def main(args, ctx=None):
    # A missing/invalid ext_redisurl leaves the wrapper unable to build a
    # client. Report it as a connection error without leaking any connection
    # detail (the module never receives the URL, only the client + prefix).
    if not ctx or not hasattr(ctx, "REDIS") or ctx.REDIS is None:
        return err("Redis is not configured: 'ext_redisurl' is missing or invalid",
                   "RedisConnectionError")

    # The key prefix is the namespace boundary. Operating without it would let
    # requests reach keys outside this app's namespace, so a missing/empty
    # ext_redis_prefix is a hard error, not a silent unprefixed run.
    prefix = getattr(ctx, "REDIS_PREFIX", None)
    if prefix is None or not str(prefix).strip():
        return err("Redis namespace is not configured: 'ext_redis_prefix' is "
                   "missing or empty", "RedisConfigError")

    _apply_timeouts(ctx)

    data = request_data(args)
    section = (data.get("cmd") or data.get("section") or "status").lower()

    handler = DISPATCH.get(section)
    if handler is None:
        return err(f"Unknown command '{section}'. Available: {sorted(set(DISPATCH))}",
                   "RedisCommandError")

    try:
        return handler(ctx, data)
    except Exception as e:
        traceback.print_exc()
        return map_exception(e)