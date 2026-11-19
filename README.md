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
`fn` and the push run without blocking `query()`. If Redis loses the data (flush, failover to an
empty replica), clients keep their local copy and the next write re-seeds Redis. `query()` is
read-only. Snapshots above `max_size` bytes uncompressed are rejected with `ValueError`, and
Redis is left unchanged.

Write lock: `write()` and `batch()` take a lock in Redis (`<name>:lock`) for the whole write, and
sync to the latest snapshot once they have it. The lock is tried once: if another client holds it,
they raise `Conflict` at once instead of waiting. Pass `lock=False` only when you are the single
writer; it skips the lock and a `batch()` then overwrites whatever other clients wrote. The lock
expires after `lock_timeout` seconds (default 30) so a crashed client can't block everyone. A write
that runs longer than that lets a second writer in, so set `lock_timeout` above your longest write.

Batches: `with db.batch() as c:` gives you a private copy of the DB. On exit it is published as a
single Overwrite: last write wins, with no compare-and-set. If the block raises, the copy is
thrown away and nothing changes. `query()` keeps returning the old data and sync keeps running
while the block is open. With `lock=False`, a batch logs a warning when it erases another client's
write (it still overwrites). A batch above `max_size` is rejected like a write.

```python
with db.batch() as c:
    c.execute("create table if not exists t(x)")
    for row in rows:
        c.execute("insert into t values (?)", row)
```

Limits: writers are serialized through Redis (a held lock means `Conflict`), the whole DB moves
on every write, Redis caps a value at 512MB. Fits read-heavy data that changes rarely.
The redis client must not use `decode_responses=True` and must keep a `socket_timeout`
(redis-py's default is 5s). Anyone who can write the Redis keys controls every client's data.

Tests: `uv run pytest` (needs `redis-server` on PATH).

Design: terms in [GLOSSARY.md](GLOSSARY.md), decisions in [docs/adr/](docs/adr/), planned work in [todo.md](todo.md).
