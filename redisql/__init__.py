import logging
import sqlite3
import threading
import uuid
import zlib

log = logging.getLogger(__name__)

# Push only if Redis still holds the version we built on. Atomic, so a stale writer can't clobber.
_PUSH = """
if (redis.call('get', KEYS[2]) or '') ~= ARGV[1] then return 0 end
redis.call('set', KEYS[1], ARGV[2])
redis.call('set', KEYS[2], ARGV[3])
redis.call('publish', KEYS[3], ARGV[3])
return 1
"""


class Conflict(Exception):
    """Another client kept writing first; write() gave up after `retries` attempts."""


class RediSQL:
    """In-memory sqlite, replicated to other clients through a Redis snapshot.

    The redis client must not use decode_responses (snapshots are bytes) and must keep a
    socket_timeout (redis-py 8 defaults to 5s), otherwise a partition can hang the listener.
    """

    def __init__(self, redis, name="redisql", poll=5, max_size=1 << 30, retries=5):
        self.redis, self.poll, self.max_size, self.retries = redis, poll, max_size, retries
        self._keys = [f"{name}:snap", f"{name}:ver", f"{name}:chan"]
        self._push = redis.register_script(_PUSH)
        # Versions are random tokens, not counters: a Redis flush or failover can't make
        # an old number look current again.
        self.version = b""
        self._lock = threading.RLock()
        self._db = sqlite3.connect(":memory:", check_same_thread=False)
        self._db.execute("pragma query_only=1")  # query() can't change local data behind Redis' back
        self._dirty = False  # local() changes not yet pushed; sync() leaves them alone
        self._stop = threading.Event()
        self._listener = self._pubsub = None

    def start(self, listen=True):
        if listen:
            self._pubsub = self.redis.pubsub()
            self._pubsub.subscribe(self._keys[2])
            self._listener = threading.Thread(target=self._listen, daemon=True)
            self._listener.start()
        self.sync()  # after subscribing, so no update falls in the gap
        return self

    def _listen(self):
        # Pubsub is the fast path. The sync() after every wait is the guarantee:
        # it covers lost messages and reconnects, at the cost of one GET per `poll` seconds.
        while not self._stop.is_set():
            try:
                self._pubsub.get_message(timeout=self.poll)  # redis-py resubscribes on reconnect
                self.sync()
            except Exception:
                log.exception("redisql listener error, retrying")
                self._stop.wait(min(self.poll, 1))

    def query(self, sql, params=()):
        with self._lock:
            return self._db.execute(sql, params).fetchall()

    def write(self, fn):
        """Run fn(conn) on the latest snapshot and push the whole DB to Redis.

        Local data changes only after Redis accepted the new snapshot. If another client wrote first,
        sync and re-run fn (so fn may run more than once); raise Conflict after `retries` tries.
        """
        for _ in range(self.retries):
            with self._lock:
                # fn runs on a copy that replaces our DB only once Redis has it, so a failed
                # push or a conflict leaves local data untouched.
                db = sqlite3.connect(":memory:", check_same_thread=False)
                try:
                    self._db.backup(db)
                    with db:
                        fn(db)
                    db.execute("pragma query_only=1")
                    new = uuid.uuid4().hex.encode()
                    pushed = self._push(keys=self._keys, args=[self.version, zlib.compress(db.serialize()), new])
                except BaseException:
                    db.close()
                    raise
                if pushed:
                    self._db, old = db, self._db
                    old.close()
                    self.version, self._dirty = new, False
                    return
                db.close()
                if self._dirty:  # sync() won't load past unpushed local() work, so retrying can't help
                    raise Conflict("another client wrote first; snapshot() to overwrite with local changes")
            self.sync()
        raise Conflict(f"gave up after {self.retries} attempts")

    def local(self, fn):
        """Run fn(conn) on the local DB only. Nothing reaches Redis until snapshot().

        Until then sync() stops loading remote updates, so they can't drop this work.
        """
        with self._lock:
            self._dirty = True  # before fn: DDL autocommits, so even a failed fn can leave changes
            self._db.execute("pragma query_only=0")
            try:
                with self._db:
                    fn(self._db)
            finally:
                self._db.execute("pragma query_only=1")

    def snapshot(self):
        """Push the local DB to Redis in one go. Last write wins: overwrites whatever Redis holds."""
        with self._lock:
            new = uuid.uuid4().hex.encode()
            raw = self._db.serialize()
            if len(raw) > self.max_size:  # other clients would reject it on every sync
                raise ValueError(f"snapshot larger than max_size={self.max_size}")
            k = self._keys
            self.redis.pipeline().set(k[0], zlib.compress(raw)).set(k[1], new).publish(k[2], new).execute()
            self.version, self._dirty = new, False

    def sync(self):
        """Load the snapshot if Redis holds a different version than ours."""
        seen = self.version
        if self._dirty or (self.redis.get(self._keys[1]) or b"") == seen:
            return
        data, ver = self.redis.pipeline().get(self._keys[0]).get(self._keys[1]).execute()
        with self._lock:
            if self._dirty or self.version != seen:  # a local write landed meanwhile; next sync rechecks
                return
            if data is None:
                # Redis lost the snapshot (flush, failover to empty replica). Keep local data;
                # the next write re-seeds Redis and every client reloads from it.
                self.version = b""
                return
            self._db.deserialize(self._decompress(data))
            self.version = ver

    def _decompress(self, data):
        d = zlib.decompressobj()
        out = d.decompress(data, self.max_size)
        if d.unconsumed_tail:
            raise ValueError(f"snapshot larger than max_size={self.max_size}")
        return out

    def close(self):
        if self._listener:
            self._stop.set()
            self._listener.join(self.poll + 5)
            self._pubsub.close()
        self._db.close()
