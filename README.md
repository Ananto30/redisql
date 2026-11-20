# redisql

Share SQL data between processes through Redis. Each client keeps an in-memory sqlite DB.
A write pushes a compressed snapshot to Redis, and other clients reload it. Reads are local.

```python
import redis
from redisql import RediSQL

db = RediSQL(redis.Redis(), "mydb").start()
db.write(lambda c: c.execute("create table if not exists t(x)"))
db.query("select * from t")
```

## How it works

- **Updates:** pubsub delivers them instantly. A listener also checks the version every `poll`
  seconds (default 5), so a dropped message or Redis outage heals itself.
- **`write(fn)`:** runs `fn` on a copy of the latest snapshot and pushes it only if nobody wrote
  first. If someone did, it syncs and runs `fn` again, so `fn` must only touch the connection it
  gets. After `retries` attempts it raises `Conflict`.
- **`query()`:** read-only and never blocked by writes.
- **Failed push:** local data stays unchanged.
- **Redis loses its data:** clients keep their copy and the next write re-seeds Redis.
- **`max_size`:** a snapshot over it raises `ValueError` and Redis stays unchanged.

## Write lock

`write()` and `batch()` take a Redis lock. If another client holds it, they raise `Conflict`
at once instead of waiting. The lock expires after `lock_timeout` seconds (default 30), so set it
above your longest write. `lock=False` skips the lock: use it only with a single writer.

## Batches

```python
with db.batch() as c:
    c.execute("create table if not exists t(x)")
    c.executemany("insert into t values (?)", rows)
```

`c` is a private copy of the DB. When the block ends, it replaces the shared DB: last write wins,
with no conflict check. If the block raises, nothing changes.

## Async

`await db.awrite(fn)` and `async with db.abatch() as c:` work the same but don't block the event
loop. `fn` can be a plain function or an `async def`, and the `abatch` block may `await`. The
lock stays held meanwhile, so fetch your data first when you can.

```python
rows = await fetch_all()
async with db.abatch() as c:
    c.executemany("insert into t values (?)", rows)
```

## Limits

- One writer at a time. The whole DB is sent on every write, and Redis caps a value at 512MB.
  Best for read-heavy data that rarely changes.
- The redis client needs `decode_responses=False` and a `socket_timeout`.
- Anyone who can write the Redis keys controls every client's data.

## Development

`uv run pytest` (needs `redis-server` on PATH). Lint and format: `uv run ruff check --fix . && uv run ruff format .`.
See [GLOSSARY.md](GLOSSARY.md),
[docs/adr/](docs/adr/) and [todo.md](todo.md).
