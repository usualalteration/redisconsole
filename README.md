# Redis Console

An interactive Redis console for **learning, exploring, and testing** Redis
functionality, built with React + Tailwind on the frontend and Nuvolaris /
Apache OpenServerless actions on the backend. All Redis operations go through a
single public action, `v1/console`, that uses the OpenServerless-provided Redis
wiring (`ctx.REDIS` / `ctx.REDIS_PREFIX`).

## Redis configuration

The backend connects using the OpenServerless environment, never hardcoded
parameters:

- `ext_redisurl` — the Redis connection URL (may carry credentials and
  connection parameters). Used to build the `redis.Redis` client.
- `ext_redis_prefix` — the key prefix that namespaces this app's keys.

`console.resolve_redis_config` prefers these `ext_*` variables and falls back to
the platform `REDIS_URL` / `REDIS_PREFIX` wiring when they are absent, so the
app runs unchanged on deployments that expose either name. The prefix is applied
transparently on the backend and stripped before keys reach the browser, so the
UI shows clean names while every Redis access stays inside the app namespace.
`ext_redisurl` is never sent to the browser; a missing/invalid URL or a
missing/empty prefix is reported as an explicit error instead of connecting
without a namespace.

## Architecture

- **Frontend** (`src/`): React + TypeScript + Tailwind, HashRouter, shadcn/ui.
  - `src/lib/api.ts` — fetch helper that unwraps the OpenServerless `{body}`
    envelope.
  - `src/lib/sections.tsx` — the sidebar section registry (add a section here +
    a page + a route + a dispatch handler to extend the console).
  - `src/components/` — `AppLayout`, `Sidebar`, `Header`, `ConnectionBadge`,
    `SectionShell` (shared command-area + output-panel layout), `OutputPanel`.
  - `src/pages/sections/` — one page per Redis feature area.
- **Backend** (`packages/v1/console/console.py`): a single dispatch action that
  routes on a `cmd` field to handlers grouped by Redis data structure. Every
  key is built from `ctx.REDIS_PREFIX`; the generated `__main__.py` wrapper is
  left untouched.

## Sections

Overview · Keys · Laboratories · Strings · Hashes · Lists · Sets · Sorted Sets ·
Streams · Pub/Sub · TTL · Transactions · Console · Reference

### Overview — Live Dashboard

The Overview section (`/overview`) is a read-only dashboard fed by Redis
`INFO` and `DBSIZE` through the `v1/console` `status` action (`detail: true`).
It surfaces: connection status, Redis version, uptime, connected clients, used
memory (with peak), total keys (`DBSIZE`), current database (with keyspace
breakdown), hit rate (from `keyspace_hits`/`keyspace_misses`), expired keys,
and evicted keys. Hit rate is tone-coded (green ≥ 80%, amber ≥ 50%, red below).

### Reference — Educational Command Reference

The Reference section (`/reference`) is a static, frontend-only curated
reference of common Redis commands (`src/lib/redis-commands.ts`). It never
calls Redis. Users filter by category (Generic, String, Hash, List, Set,
Sorted Set, Stream, Pub/Sub, Transactions, Server) and search by
command/description/syntax. Each entry expands to show the syntax,
argument explanations (with optional markers), and a worked example with its
expected output. Commands are rendered with token-level syntax highlighting.

### Console — Safe Raw Redis Terminal

The Console section (`/raw`, labelled “Console” in the sidebar) is a
terminal-like raw Redis console. The browser sends each typed command to the
`v1/console` action with `cmd: "raw"`; the frontend never connects to Redis
directly.

Frontend behaviour:

- Command history persisted for the browser session (`localStorage`), with
  Up/Down arrow navigation.
- Enter executes the command.
- Multi-line, redis-cli-style output rendering (quoted strings, numbered
  array elements, `(nil)` for null).
- Execution duration shown per command (backend-measured `duration_ms`).
- Errors rendered in red.
- Clear-console button (and a separate clear-history button).

Backend behaviour (`packages/v1/console/console.py`, `cmd_raw`):

- Commands are parsed with `shlex` so quoted arguments such as
  `SET message "hello world"` are handled correctly. No `eval` or shell
  execution is used.
- A backend-authoritative denylist blocks destructive / server-compromising
  commands: `FLUSHALL`, `FLUSHDB`, `SHUTDOWN`, `CONFIG`, `DEBUG`, `MODULE`,
  `SLAVEOF`, `REPLICAOF`, `MIGRATE`, `RESTORE`, `ACL`, `SCRIPT`, `FUNCTION`,
  and `KEYS` (use SCAN instead). The same denylist is enforced on the
  transactions endpoint.
- The denylist cannot be bypassed from the frontend: a browser-supplied
  `allow_dangerous` / `allow_meta` flag is ignored. An operator can explicitly
  enable the blocked commands only through backend configuration — the
  environment variable `REDIS_CONSOLE_ALLOW_DANGEROUS` or a bound action
  parameter `ALLOW_DANGEROUS` set to a truthy value.
- The first key argument is automatically namespaced with the app prefix; no
  meta/no-key commands receive a prefix.
- The Redis execution duration is measured and returned as `duration_ms`.

### Keys — Visual Key Explorer

The Keys section is a visual browser, not a command form:

- Search/filter by a glob `pattern` with quick chips (`*`, `user:*`, `session:*`,
  `cache:*`), a refresh button, and a live key count (loaded so far + DB size).
- Keys are loaded with a cursor-based `SCAN` (never `KEYS *`), `count` per page,
  with a **Load more** button that continues the cursor. Each row shows the
  key name, Redis `TYPE`, and `TTL`.
- Selecting a key detects its type and opens a type-aware inspector with safe,
  bounded reads (never the whole huge structure at once):
  - **String**: value, length, edit (`SET`).
  - **Hash**: fields via `HSCAN` (paginated), field count (`HLEN`), add/edit/remove
    field (`HSET`/`HDEL`).
  - **List**: index/value via `LRANGE` (pageable), length (`LLEN`), `LPUSH`/
    `RPUSH`/`LPOP`/`RPOP`.
  - **Set**: members via `SSCAN` (paginated), cardinality (`SCARD`), add/remove
    member (`SADD`/`SREM`).
  - **Sorted Set**: rank/member/score via `ZRANGE ... WITHSCORES` (pageable),
    cardinality (`ZCARD`), add/update (`ZADD`), remove (`ZREM`).
  - **Stream**: entry id/fields via `XRANGE` (count-bounded), length (`XLEN`),
    add entry (`XADD`).
- Every visual operation displays the equivalent raw Redis command so the
  console doubles as a Redis learning tool.
- Deleting a key asks for confirmation and runs `DEL`.

The backend gained three ops on `v1/console`: `keys.scan` accepts `withmeta`
to return `{name,type,ttl}` per key, and `hashes.hscan` / `sets.sscan` provide
cursor-based, bounded iteration over large hashes/sets instead of `HGETALL` /
`SMEMBERS`.

The UI is responsive (desktop sidebar, mobile drawer) and includes a Redis
connection/status indicator, a command execution area, and an output/result
panel for each section.

## Notes / limitations

- All keys are automatically namespaced with the app's Redis prefix
  (`ext_redis_prefix`, e.g. `redisconsole:`). The frontend never sees the
  prefix; the backend adds it exactly once on every access.
- Pub/Sub `PUBLISH` may be restricted by the platform Redis ACL; `SUBSCRIBE`
  requires a long-lived connection that serverless actions cannot hold, so only
  publish + channel discovery are exposed. The Pub/Sub section explains this
  limitation in the UI rather than faking subscription behavior.
- Transactions (`/transactions`) demonstrate `MULTI`/`EXEC`, `MULTI`/`DISCARD`,
  and `WATCH`/`MULTI`/`EXEC`. Because OpenServerless requests may use separate
  Redis connections, the whole transaction runs in a single backend action on
  one redis-py pipeline / connection so the `WATCH` lock is held by the same
  client that issues `EXEC`. A built-in "interfere" toggle mutates the watched
  key between `WATCH` and `EXEC` on that same connection to deterministically
  demonstrate the optimistic-lock abort path.
- Application `.env` / `.env.production` are managed by Trustable, not by this
  code.