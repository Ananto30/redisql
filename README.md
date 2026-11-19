# redisql

Run SQL over data shared through Redis. Each client keeps an in-memory sqlite DB.
A write pushes the compressed DB snapshot to Redis and publishes the new version;
other clients reload it. Reads are local and never hit the network.

```python
import redis
from redisql import RediSQL

db = RediSQL(redis.Redis(), "mydb", poll=5).start()   # loads snapshot, listens for updates
db.write(lambda c: c.execute("create table if not exists t(x)"))
db.query("select * from t")
```

Delivery: pubsub pushes updates instantly. Pubsub can drop messages, so the listener thread
also checks the Redis version every `poll` seconds, which reloads any missed update and
recovers from Redis outages. Worst-case staleness is `poll` seconds plus `socket_timeout`.

Writes: `fn` runs on a copy of the latest snapshot. The copy is pushed with a compare-and-set
on the version, and replaces the local DB only once Redis accepted it, so a failed push changes
nothing. If another client wrote first, `write` syncs and re-runs `fn` (so `fn` may run more than
once and must only touch the connection it's given), raising `Conflict` after `retries` attempts.
If Redis loses the data (flush, failover to an empty replica), clients keep their local copy and
the next write re-seeds Redis. `query()` is read-only. Snapshots above `max_size` bytes
uncompressed are rejected.

Bulk writes: `local(fn)` changes only the local DB (call it as often as you like), then
`snapshot()` pushes the whole DB once. Last write wins: `snapshot()` overwrites whatever Redis
holds, including other clients' writes made meanwhile. While local changes are unpushed, `sync()`
stops loading remote updates so they can't drop your work, and `write()` raises `Conflict` at once if
another client wrote first; a successful `write()` pushes pending local changes too. `snapshot()`
refuses a DB above `max_size`.

```python
db.local(lambda c: c.execute("create table if not exists t(x)"))
for row in rows:
    db.local(lambda c: c.execute("insert into t values (?)", row))
db.snapshot()
```

Limits: writers are serialized through Redis (contention means retries), the whole DB moves
on every write and the lock blocks reads while it compresses,
Redis caps a value at 512MB. Fits read-heavy data that changes rarely.
The redis client must not use `decode_responses=True` and must keep a `socket_timeout`
(redis-py's default is 5s). Anyone who can write the Redis keys controls every client's data.

Tests: `uv run pytest` (needs `redis-server` on PATH).
