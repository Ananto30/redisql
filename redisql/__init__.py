import asyncio
import contextlib
import inspect
import logging
import sqlite3
import sys
import threading
import uuid
import zlib

from redis.exceptions import LockError

log = logging.getLogger(__name__)

# Compare-and-set push. KEYS: snapshot, version, channel. ARGV: expected version, new snapshot,
# new version. Runs atomically in Redis, so a stale writer can't overwrite a newer snapshot.
# Returns 1 if pushed, 0 if Redis holds a different version than expected.
_PUSH = """
if (redis.call('get', KEYS[2]) or '') ~= ARGV[1] then return 0 end
redis.call('set', KEYS[1], ARGV[2])
redis.call('set', KEYS[2], ARGV[3])
redis.call('publish', KEYS[3], ARGV[3])
return 1
"""


class Conflict(Exception):
    """Write was refused.

    Either another client holds the Write lock, or other clients kept writing first and
    write() gave up after `retries` attempts.
    """


class RediSQL:
    """In-memory sqlite, replicated to other clients through a Redis snapshot.

    The redis client must not use decode_responses (snapshots are bytes) and must keep a
    socket_timeout (redis-py 8 defaults to 5s), otherwise a partition can hang the listener.
    """

    def __init__(self, redis, name="redisql", poll=5, max_size=1 << 30, retries=5, lock_timeout=30):
        self.redis, self.poll, self.max_size, self.retries = redis, poll, max_size, retries
        # Set above your longest write, or the lock expires and a second writer can get in.
        self.lock_timeout = lock_timeout
        self._lock_key = f"{name}:lock"
        self._keys = [f"{name}:snap", f"{name}:ver", f"{name}:chan"]
        self._push = redis.register_script(_PUSH)
        # Versions are random tokens, not counters: a Redis flush or failover can't make
        # an old number look current again.
        self.version = b""
        self._lock = threading.RLock()
        self._db = sqlite3.connect(":memory:", check_same_thread=False)
        # Local data may only change through Redis, so query() can't write.
        self._db.execute("pragma query_only=1")
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

    def write(self, fn, lock=True):
        """Run fn(conn) on the latest snapshot and push the whole DB to Redis.

        Takes the Write lock and raises Conflict at once if another client holds it; lock=False
        skips it (only safe with a single writer). Local data changes only after Redis accepted
        the new snapshot. If another client wrote first, sync and re-run fn (so fn may run more
        than once); raise Conflict after `retries` tries.
        """
        with self._write_lock(lock):
            for _ in range(self.retries):
                pushed = self._copy_and_push(fn)
                if pushed:
                    db, new, base = pushed
                    with self._lock:
                        # If sync() already loaded a later version, it includes our push; keep that.
                        if self.version == base:
                            self._db, old = db, self._db
                            old.close()
                            self.version = new
                        else:
                            db.close()
                    return
                self.sync()
        raise Conflict(f"gave up after {self.retries} attempts")

    @contextlib.contextmanager
    def _write_lock(self, lock):
        if not lock:
            yield
            return
        # thread_local=False: abatch() acquires and releases from different worker threads
        redis_lock = self.redis.lock(self._lock_key, timeout=self.lock_timeout, thread_local=False)
        if not redis_lock.acquire(blocking=False):
            raise Conflict("another client holds the write lock")
        try:
            self.sync()  # so the first attempt builds on current data
            yield
        finally:
            with contextlib.suppress(LockError):  # lock_timeout passed: nothing left to release
                redis_lock.release()

    def _copy_and_push(self, fn):
        """Run fn on a copy of the local DB and push it if Redis still holds the version we copied.

        Returns (copy, new version, base version), or None if another client wrote first.
        The local lock is held only while copying, so query() isn't blocked while fn runs or
        the push happens.
        """
        db, base = self._copy()
        try:
            with db:
                fn(db)
            new = uuid.uuid4().hex.encode()
            if self._push(keys=self._keys, args=[base, self._pack(db), new]):
                return db, new, base
        except BaseException:
            db.close()
            raise
        db.close()

    @contextlib.contextmanager
    def batch(self, lock=True):
        """Yield a private copy of the local DB; on exit publish it as an Overwrite.

        Overwrite means last write wins, with no version check. Holds the Write lock for the
        whole block and raises Conflict at once if another client holds it; lock=False skips it
        (only safe with a single writer). If the block raises, the copy is thrown away.
        query() and sync() keep working meanwhile.
        """
        with self._write_lock(lock):
            db, base = self._copy()
            try:
                with db:  # commits on success, rolls back if the block raises
                    yield db
                raw, new, k = self._pack(db), uuid.uuid4().hex.encode(), self._keys
                if (self.redis.get(k[1]) or b"") != base:
                    log.warning("batch overwrites changes another client made since it started")
                self.redis.pipeline().set(k[0], raw).set(k[1], new).publish(k[2], new).execute()
            except BaseException:
                db.close()
                raise
            with self._lock:
                self._db, old = db, self._db
                old.close()
                self.version = new

    async def awrite(self, fn, lock=True):
        """write() without blocking the event loop. fn may be a plain function or an `async def`.

        An async fn runs on the caller's event loop (so it can await your async clients) while
        the Redis I/O stays in a worker thread. Like write(), fn may run more than once.
        """
        if inspect.iscoroutinefunction(fn):
            loop, afn = asyncio.get_running_loop(), fn

            def fn(db):
                # write() runs in a worker thread; hand the coroutine back to the loop and wait.
                return asyncio.run_coroutine_threadsafe(afn(db), loop).result()

        await asyncio.to_thread(self.write, fn, lock)

    @contextlib.asynccontextmanager
    async def abatch(self, lock=True):
        """batch() for async code: `async with db.abatch() as c:` lets the block await.

        Redis I/O runs in worker threads, so the event loop stays free. Use c (a plain sqlite
        connection) only from the block itself. The Write lock is held while the block awaits,
        so keep awaits short compared to lock_timeout.
        """
        # Drive the sync context manager by hand so its blocking enter/exit run off the loop.
        cm = self.batch(lock)
        db = await asyncio.to_thread(cm.__enter__)
        try:
            yield db
        except BaseException:
            # Let batch() see the error so it discards the copy and releases the lock.
            await asyncio.to_thread(cm.__exit__, *sys.exc_info())
            raise
        await asyncio.to_thread(cm.__exit__, None, None, None)

    def _copy(self):
        db = sqlite3.connect(":memory:", check_same_thread=False)
        with self._lock:
            self._db.backup(db)
            return db, self.version

    def _pack(self, db):
        """Freeze db as the next local DB and return its compressed snapshot."""
        db.execute("pragma query_only=1")
        raw = db.serialize()
        if len(raw) > self.max_size:  # other clients would reject it on every sync
            raise ValueError(f"snapshot larger than max_size={self.max_size}")
        return zlib.compress(raw)

    def sync(self):
        """Load the snapshot if Redis holds a different version than ours."""
        seen = self.version
        if (self.redis.get(self._keys[1]) or b"") == seen:
            return
        data, ver = self.redis.pipeline().get(self._keys[0]).get(self._keys[1]).execute()
        with self._lock:
            if self.version != seen:  # a local write landed meanwhile; next sync rechecks
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
