# redisql

SQL interface for Redis. Each client keeps an in-memory sqlite DB. Aimed at read-heavy workloads with occasional writes.

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
- **`stats()`:** a dict of local state for health endpoints: `version`, `size_bytes`, `tables`,
  `listening`, `synced_ago`, `changed_ago` (seconds) and `last_error`. It makes no Redis call.
  A `synced_ago` well above `poll` means the client can't reach Redis.
- **`fleet_stats()`:** `{"total": ..., "workers": {host:pid:id: stats}}` for every listening
  client, read from Redis. `total` has `workers`, `versions`, `in_sync`, `size_bytes` (summed
  memory) and `with_errors`. Each client republishes every `poll` seconds, so numbers are up to
  `poll` old, and a worker that dies drops out after 3 polls. `listen=False` clients don't appear.
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

## Read benchmark

With Redis running, measure local read throughput at several thread concurrency levels:

```bash
uv run python -m benchmarks.read_concurrency \
  --concurrency 1,4,16,64 \
  --duration 10 \
  --rows 100000 \
  --output read-benchmark.csv
```

The CSV records completed requests, errors, RPS, and latency percentiles for each concurrency
level. This measures threads inside one process; repeat it with the same CPU and data size as the
FastAPI deployment being evaluated.

## Product catalog example

With Redis running locally, run `uv run python -m examples.product_catalog` from the repo root.
Set `REDIS_URL` if Redis is elsewhere.

Follow `run_demo()` in [product_catalog.py](examples/product_catalog.py) alongside the output:

1. The shop inserts 10,000 products in one `batch()`, which publishes one snapshot.
2. The storefront loads that snapshot and uses `query()` on its local copy. It filters
   by name and color, sorts by price, and prints the three cheapest red trail backpacks.
3. The shop uses `write()` to change Trail Backpack 0001 from blue to red.
4. The storefront calls `sync()` to load the updated snapshot without waiting
   for Pub/Sub. The same query now lists Trail Backpack 0001 first, at $20.00.

## Development

`uv run pytest` (needs `redis-server` on PATH). Lint and format: `uv run ruff check --fix . && uv run ruff format .`.
See [GLOSSARY.md](GLOSSARY.md),
[docs/adr/](docs/adr/) and [todo.md](todo.md).
