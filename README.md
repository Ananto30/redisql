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
recovers from Redis outages. Worst-case staleness is `poll` seconds. `close()` can block up to `poll` seconds.

Limits: last write wins (no locking), the whole DB moves on every write,
Redis caps a value at 512MB. Fits read-heavy data that changes rarely.
The redis client must not use `decode_responses=True`.

Tests: `uv run pytest` (needs `redis-server` on PATH).
