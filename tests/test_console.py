"""
Unit tests for the Redis Console backend action (packages/v1/console/console.py).

These tests are dependency-free on purpose: they do NOT import the real
``redis`` package, do NOT require ``pytest``, and never open a network
connection. They run with a plain interpreter:

    python3 tests/test_console.py

They cover the OpenServerless Redis integration requirements:

1. Connections are built from ``ext_redisurl`` (with REDIS_URL fallback).
2. Every Redis operation respects ``ext_redis_prefix``.
3. The prefix is applied exactly once.
4. Keys outside the configured namespace are inaccessible.
5. Redis credentials are never exposed to the frontend.
6. Existing Redis Console functionality continues to work.
"""

import fnmatch
import importlib.util
import json
import os
import sys
import types
import unittest

# Import console.py by path without importing the real redis package (console
# only imports redis lazily, inside functions, so module import is safe here).
_CONSOLE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "packages", "v1", "console",
)
_spec = importlib.util.spec_from_file_location(
    "console", os.path.join(_CONSOLE_DIR, "console.py")
)
console = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(console)


class FakeRedis:
    """Minimal in-memory Redis stand-in covering the commands under test.

    ``touched`` records every full key name the module hands to the client, so
    tests can assert that the app prefix is always applied. The store is a flat
    dict keyed by the *full* (prefixed) key name, mirroring a real keyspace in
    which unrelated namespaces coexist.
    """

    def __init__(self):
        self.store = {}          # full_key -> value (str | dict | list | set)
        self.touched = set()     # every full key the module accessed

    # -- helpers ----------------------------------------------------------
    def _hit(self, *keys):
        for k in keys:
            self.touched.add(k)

    # -- connection / generic --------------------------------------------
    def ping(self):
        return True

    def dbsize(self):
        return len(self.store)

    def scan(self, cursor=0, match=None, count=None):
        pat = match or "*"
        keys = [k for k in self.store if fnmatch.fnmatchcase(k, pat)]
        return (0, keys)

    def type(self, name):
        self._hit(name)
        v = self.store.get(name)
        if v is None:
            return "none"
        if isinstance(v, dict):
            return "hash"
        if isinstance(v, list):
            return "list"
        if isinstance(v, set):
            return "set"
        return "string"

    def ttl(self, name):
        self._hit(name)
        return -1

    def exists(self, *names):
        self._hit(*names)
        return sum(1 for n in names if n in self.store)

    def delete(self, *names):
        self._hit(*names)
        n = 0
        for name in names:
            if name in self.store:
                del self.store[name]
                n += 1
        return n

    def rename(self, src, dst):
        self._hit(src, dst)
        self.store[dst] = self.store.pop(src)
        return True

    # -- strings ----------------------------------------------------------
    def set(self, name, value, ex=None, nx=False, xx=False):
        self._hit(name)
        exists = name in self.store
        if nx and exists:
            return None
        if xx and not exists:
            return None
        self.store[name] = str(value)
        return True

    def get(self, name):
        self._hit(name)
        return self.store.get(name)

    def getset(self, name, value):
        self._hit(name)
        old = self.store.get(name)
        self.store[name] = str(value)
        return old

    def incr(self, name, amount=1):
        self._hit(name)
        self.store[name] = str(int(self.store.get(name, "0")) + amount)
        return int(self.store[name])

    def strlen(self, name):
        self._hit(name)
        return len(self.store.get(name, ""))

    def mget(self, names):
        self._hit(*names)
        return [self.store.get(n) for n in names]

    # -- hashes -----------------------------------------------------------
    def hset(self, name, key=None, value=None, mapping=None):
        self._hit(name)
        h = self.store.setdefault(name, {})
        added = 0
        if mapping:
            for k, v in mapping.items():
                if k not in h:
                    added += 1
                h[k] = str(v)
        if key is not None:
            if key not in h:
                added += 1
            h[key] = str(value)
        return added

    def hgetall(self, name):
        self._hit(name)
        return dict(self.store.get(name, {}))

    def hget(self, name, key):
        self._hit(name)
        return self.store.get(name, {}).get(key)

    def hlen(self, name):
        self._hit(name)
        return len(self.store.get(name, {}))

    # -- sets -------------------------------------------------------------
    def sadd(self, name, *members):
        self._hit(name)
        s = self.store.setdefault(name, set())
        before = len(s)
        s.update(str(m) for m in members)
        return len(s) - before

    def smembers(self, name):
        self._hit(name)
        return set(self.store.get(name, set()))

    def scard(self, name):
        self._hit(name)
        return len(self.store.get(name, set()))

    # -- lists ------------------------------------------------------------
    def rpush(self, name, *values):
        self._hit(name)
        lst = self.store.setdefault(name, [])
        lst.extend(str(v) for v in values)
        return len(lst)

    def lrange(self, name, start, stop):
        self._hit(name)
        lst = self.store.get(name, [])
        if stop == -1:
            return lst[start:]
        return lst[start:stop + 1]

    def llen(self, name):
        self._hit(name)
        return len(self.store.get(name, []))


PREFIX = "app:"


def make_ctx(prefix=PREFIX, client=None):
    ctx = types.SimpleNamespace()
    ctx.REDIS = client if client is not None else FakeRedis()
    ctx.REDIS_PREFIX = prefix
    return ctx


class TestResolveRedisConfig(unittest.TestCase):
    """(1) Connections are established from ext_redisurl / ext_redis_prefix."""

    def test_prefers_ext_variables(self):
        env = {
            "ext_redisurl": "redis://ext-host:6379/0",
            "ext_redis_prefix": "extprefix:",
            "REDIS_URL": "redis://legacy:6379/0",
            "REDIS_PREFIX": "legacy:",
        }
        url, prefix = console.resolve_redis_config({}, env)
        self.assertEqual(url, "redis://ext-host:6379/0")
        self.assertEqual(prefix, "extprefix:")

    def test_falls_back_to_platform_wiring(self):
        env = {"REDIS_URL": "redis://legacy:6379/0", "REDIS_PREFIX": "legacy:"}
        url, prefix = console.resolve_redis_config({}, env)
        self.assertEqual(url, "redis://legacy:6379/0")
        self.assertEqual(prefix, "legacy:")

    def test_empty_ext_value_falls_back(self):
        # An unset $ext_redisurl param arrives as "" and must not win.
        env = {"ext_redisurl": "", "REDIS_URL": "redis://legacy:6379/0",
               "ext_redis_prefix": "", "REDIS_PREFIX": "legacy:"}
        url, prefix = console.resolve_redis_config({}, env)
        self.assertEqual(url, "redis://legacy:6379/0")
        self.assertEqual(prefix, "legacy:")

    def test_bound_param_wins_over_environ(self):
        args = {"ext_redisurl": "redis://param:6379/0"}
        env = {"ext_redisurl": "redis://env:6379/0"}
        url, _ = console.resolve_redis_config(args, env)
        self.assertEqual(url, "redis://param:6379/0")

    def test_missing_everything_returns_none(self):
        url, prefix = console.resolve_redis_config({}, {})
        self.assertIsNone(url)
        self.assertIsNone(prefix)


class TestPrefixEnforcement(unittest.TestCase):
    """(2) Every operation respects the prefix. (3) Applied exactly once."""

    def _run_battery(self, ctx):
        ops = [
            {"cmd": "strings", "op": "set", "key": "s1", "value": "v"},
            {"cmd": "strings", "op": "get", "key": "s1"},
            {"cmd": "strings", "op": "incr", "key": "counter"},
            {"cmd": "hashes", "op": "hset", "key": "h1", "field": "f", "value": "v"},
            {"cmd": "hashes", "op": "hgetall", "key": "h1"},
            {"cmd": "sets", "op": "sadd", "key": "set1", "members": ["a", "b"]},
            {"cmd": "sets", "op": "smembers", "key": "set1"},
            {"cmd": "lists", "op": "rpush", "key": "list1", "values": ["x"]},
            {"cmd": "lists", "op": "lrange", "key": "list1"},
            {"cmd": "keys", "op": "scan", "pattern": "*", "withmeta": True},
            {"cmd": "keys", "op": "type", "key": "s1"},
            {"cmd": "keys", "op": "exists", "key": "s1"},
            {"cmd": "keys", "op": "del", "key": "counter"},
        ]
        for payload in ops:
            res = console.main(payload, ctx=ctx)
            self.assertTrue(res.get("ok"), f"op failed: {payload} -> {res}")

    def test_every_touched_key_is_prefixed(self):
        ctx = make_ctx()
        self._run_battery(ctx)
        self.assertTrue(ctx.REDIS.touched, "no keys were touched")
        for key in ctx.REDIS.touched:
            self.assertTrue(
                key.startswith(PREFIX),
                f"key {key!r} escaped the {PREFIX!r} namespace",
            )

    def test_prefix_applied_exactly_once(self):
        ctx = make_ctx()
        # rkey is idempotent: applying it to an already-prefixed key is a no-op.
        once = console.rkey(ctx, "user:1")
        twice = console.rkey(ctx, once)
        self.assertEqual(once, "app:user:1")
        self.assertEqual(once, twice)
        self.assertFalse(twice.startswith("app:app:"))

        # A set followed by get through the module must resolve to one prefix.
        console.main({"cmd": "strings", "op": "set", "key": "only", "value": "1"},
                     ctx=ctx)
        self.assertIn("app:only", ctx.REDIS.store)
        self.assertNotIn("app:app:only", ctx.REDIS.store)

    def test_returned_keys_are_stripped_for_the_frontend(self):
        ctx = make_ctx()
        console.main({"cmd": "strings", "op": "set", "key": "visible", "value": "1"},
                     ctx=ctx)
        res = console.main({"cmd": "keys", "op": "scan", "pattern": "*"}, ctx=ctx)
        self.assertIn("visible", res["keys"])
        for name in res["keys"]:
            self.assertFalse(name.startswith(PREFIX),
                             f"frontend saw prefixed key {name!r}")


class TestNamespaceIsolation(unittest.TestCase):
    """(4) Keys outside the configured namespace are inaccessible."""

    def _seed_foreign_key(self):
        client = FakeRedis()
        client.store["other:secret"] = "top-secret"      # different namespace
        client.store["app:mine"] = "ok"
        return client

    def test_scan_never_reveals_foreign_keys(self):
        ctx = make_ctx(client=self._seed_foreign_key())
        res = console.main({"cmd": "keys", "op": "scan", "pattern": "*"}, ctx=ctx)
        self.assertEqual(res["keys"], ["mine"])
        self.assertNotIn("other:secret", res["keys"])
        self.assertNotIn("secret", res["keys"])

    def test_get_cannot_reach_foreign_key(self):
        client = self._seed_foreign_key()
        ctx = make_ctx(client=client)
        # Asking for "other:secret" is namespaced to app:other:secret (absent).
        res = console.main({"cmd": "strings", "op": "get", "key": "other:secret"},
                           ctx=ctx)
        self.assertTrue(res["ok"])
        self.assertIsNone(res["value"])
        # The real foreign key is untouched.
        self.assertEqual(client.store["other:secret"], "top-secret")

    def test_delete_cannot_remove_foreign_key(self):
        client = self._seed_foreign_key()
        ctx = make_ctx(client=client)
        console.main({"cmd": "keys", "op": "del", "key": "other:secret"}, ctx=ctx)
        self.assertIn("other:secret", client.store)  # still there


class TestErrorHandling(unittest.TestCase):
    """(5-partial) Missing/invalid configuration is reported, not silently used."""

    def test_missing_client_is_connection_error(self):
        ctx = make_ctx()
        ctx.REDIS = None
        res = console.main({"cmd": "status"}, ctx=ctx)
        self.assertFalse(res["ok"])
        self.assertEqual(res["error"]["type"], "RedisConnectionError")
        self.assertIn("ext_redisurl", res["error"]["message"])

    def test_empty_prefix_is_config_error(self):
        for bad in (None, "", "   "):
            ctx = make_ctx(prefix=bad)
            res = console.main({"cmd": "status"}, ctx=ctx)
            self.assertFalse(res["ok"], f"prefix {bad!r} should be rejected")
            self.assertEqual(res["error"]["type"], "RedisConfigError")
            self.assertIn("ext_redis_prefix", res["error"]["message"])

    def test_no_ctx_is_connection_error(self):
        res = console.main({"cmd": "status"}, ctx=None)
        self.assertFalse(res["ok"])
        self.assertEqual(res["error"]["type"], "RedisConnectionError")


class TestCredentialsNeverExposed(unittest.TestCase):
    """(5) Redis credentials are never exposed to the frontend."""

    SECRET_URL = "redis://user:sup3rs3cret@redis-host:6379/0"

    def test_status_response_has_no_credentials(self):
        ctx = make_ctx()
        res = console.main({"cmd": "status"}, ctx=ctx)
        blob = json.dumps(res)
        self.assertNotIn("sup3rs3cret", blob)
        self.assertNotIn("redis-host", blob)
        self.assertNotIn(self.SECRET_URL, blob)

    def test_request_body_cannot_inject_connection_vars(self):
        # A malicious frontend cannot override the URL/prefix via the body.
        data = console.request_data({
            "cmd": "status",
            "ext_redisurl": self.SECRET_URL,
            "ext_redis_prefix": "evil:",
            "REDIS_URL": self.SECRET_URL,
            "REDIS_PREFIX": "evil:",
        })
        self.assertNotIn("ext_redisurl", data)
        self.assertNotIn("ext_redis_prefix", data)
        self.assertNotIn("REDIS_URL", data)
        self.assertNotIn("REDIS_PREFIX", data)

    def test_module_never_receives_the_url(self):
        # The module works purely from ctx.REDIS + ctx.REDIS_PREFIX; the URL is
        # never handed to it, so no handler can echo it back.
        ctx = make_ctx()
        self.assertFalse(hasattr(ctx.REDIS, "connection_url"))
        res = console.main({"cmd": "keys", "op": "scan"}, ctx=ctx)
        self.assertNotIn("sup3rs3cret", json.dumps(res))


class TestExistingFunctionality(unittest.TestCase):
    """(6) Existing Redis Console functionality continues to work."""

    def test_string_set_get_roundtrip(self):
        ctx = make_ctx()
        set_res = console.main(
            {"cmd": "strings", "op": "set", "key": "greeting", "value": "hi"},
            ctx=ctx)
        self.assertTrue(set_res["ok"] and set_res["set"])
        get_res = console.main(
            {"cmd": "strings", "op": "get", "key": "greeting"}, ctx=ctx)
        self.assertEqual(get_res["value"], "hi")
        self.assertEqual(get_res["key"], "greeting")  # stripped for UI

    def test_hash_roundtrip(self):
        ctx = make_ctx()
        console.main({"cmd": "hashes", "op": "hset", "key": "u:1",
                      "field": "name", "value": "Ada"}, ctx=ctx)
        res = console.main({"cmd": "hashes", "op": "hgetall", "key": "u:1"}, ctx=ctx)
        self.assertEqual(res["fields"], {"name": "Ada"})

    def test_set_roundtrip(self):
        ctx = make_ctx()
        console.main({"cmd": "sets", "op": "sadd", "key": "tags",
                      "members": ["a", "b", "a"]}, ctx=ctx)
        res = console.main({"cmd": "sets", "op": "smembers", "key": "tags"}, ctx=ctx)
        self.assertEqual(sorted(res["members"]), ["a", "b"])

    def test_unknown_command_is_error(self):
        ctx = make_ctx()
        res = console.main({"cmd": "does-not-exist"}, ctx=ctx)
        self.assertFalse(res["ok"])
        self.assertEqual(res["error"]["type"], "RedisCommandError")

    def test_scan_uses_scan_not_keys(self):
        # Regression guard: the module must not fall back to KEYS.
        with open(os.path.join(_CONSOLE_DIR, "console.py"), encoding="utf-8") as fh:
            source = fh.read()
        self.assertNotIn("r.keys(", source)
        self.assertIn("r.scan(", source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
