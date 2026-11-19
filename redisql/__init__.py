import sqlite3
import threading
import zlib


class RediSQL:
    """In-memory sqlite, replicated to other clients through a Redis snapshot.

    The redis client must not use decode_responses (snapshots are bytes).
    """

    def __init__(self, redis, name="redisql"):
        self.redis = redis
        self._snap, self._ver, self._chan = f"{name}:snap", f"{name}:ver", f"{name}:chan"
        self.version = 0
        self._lock = threading.RLock()
        self._db = sqlite3.connect(":memory:", check_same_thread=False)
        self._listener = self._pubsub = None

    def start(self, listen=True):
        if listen:
            self._pubsub = self.redis.pubsub()
            self._pubsub.subscribe(**{self._chan: lambda _msg: self.sync()})
            self._listener = self._pubsub.run_in_thread(sleep_time=0.05, daemon=True)
        self.sync()  # after subscribing, so no update falls in the gap
        return self

    def query(self, sql, params=()):
        with self._lock:
            return self._db.execute(sql, params).fetchall()

    def write(self, fn):
        """Run fn(conn) locally, then push the whole DB to Redis and notify others."""
        with self._lock:
            with self._db:  # rolls back if fn raises, so local never diverges from Redis
                fn(self._db)
            data = zlib.compress(self._db.serialize())
            # ponytail: last write wins, no CAS. Add one if writers ever race.
            _, self.version = self.redis.pipeline().set(self._snap, data).incr(self._ver).execute()
        self.redis.publish(self._chan, self.version)

    def sync(self):
        """Load the snapshot if Redis holds a newer version than ours."""
        remote = int(self.redis.get(self._ver) or 0)
        if remote == self.version:
            return
        data, ver = self.redis.pipeline().get(self._snap).get(self._ver).execute()
        if data is None:
            return
        with self._lock:
            if int(ver) > self.version:  # a local write may have landed meanwhile
                self._db.deserialize(zlib.decompress(data))
                self.version = int(ver)

    def close(self):
        if self._listener:
            self._listener.stop()
            self._listener.join()
            self._pubsub.close()
        self._db.close()
