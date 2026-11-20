import asyncio
import contextlib
import inspect
import sys

from redis.exceptions import LockError

from ._listener import Listener
from ._local import LocalCopy
from ._stats import Stats
from ._store import SnapshotStore


class Conflict(Exception):
    """Write was refused because another client holds the Write lock."""


class RediSQL:
    """
    In-memory sqlite, replicated to other clients through a Redis snapshot.

    The redis client must not use decode_responses (snapshots are bytes) and must keep a
    socket_timeout (redis-py 8 defaults to 5s), otherwise a partition can hang the listener.
    """

    def __init__(
        self,
        redis,
        name="redisql",
        poll=5,
        max_size=1 << 30,
        lock_timeout=30,
    ):
        self.redis, self.poll, self.max_size = redis, poll, max_size
        # Set above your longest write, or the lock expires and a second writer can get in.
        self.lock_timeout = lock_timeout
        self._lock_key = f"{name}:lock"
        self._store = SnapshotStore(redis, name, max_size)
        self._local = LocalCopy()
        self._listener = None
        self._stats = Stats(
            redis,
            name,
            poll,
            max_size,
            self._local,
            lambda: bool(self._listener and self._listener.alive),
        )

    @property
    def version(self):
        return self._local.version

    def start(self, listen=True):
        if listen:
            self._store.subscribe()
        self.sync()  # after subscribing, so no update falls in the gap
        if listen:
            # Pubsub is the fast path. The sync() after every wait is the guarantee:
            # it covers lost messages and reconnects, at the cost of one GET per `poll` seconds.
            self._listener = Listener(
                wait=lambda: self._store.wait(self.poll),
                on_change=self._on_change,
                on_error=self._stats.failed,
                retry=min(self.poll, 1),
            )
            self._listener.start()
            self._stats.publish()
        return self

    def _on_change(self):
        self.sync()
        self._stats.healthy()

    def query(self, sql, params=()):
        return self._local.query(sql, params)

    def stats(self):
        """
        Local state as a dict. No Redis call, so it's cheap enough for a health endpoint.

        size_bytes is the DB size, which is what the in-memory copy costs. The `*_ago` values are
        seconds, None if it hasn't happened yet: compare synced_ago to `poll` to spot a stale
        client. last_error is the listener's latest failure, cleared once it recovers.
        """
        return self._stats.report()

    def fleet_stats(self):
        """
        stats() of every listening client, from Redis: {"total": {...}, "workers": {id: stats}}.

        Each client republishes every `poll` seconds, so a worker's numbers are up to `poll` old,
        and one that stopped (crashed, partitioned) disappears after 3 polls. Clients started
        with listen=False don't publish. total.in_sync is False while versions differ, which is
        normal for a moment after each write.
        """
        return self._stats.fleet()

    def write(self, fn, lock=True):
        """
        Run fn(conn) on the latest snapshot and publish the whole DB as an Overwrite.

        Same as a one-statement batch(): takes the Write lock and raises Conflict at once if
        another client holds it; lock=False skips it (only safe with a single writer). If fn
        raises, nothing changes.
        """
        with self.batch(lock) as db:
            fn(db)

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
            self.sync()  # so the copy builds on current data
            yield
        finally:
            with contextlib.suppress(LockError):  # lock_timeout passed: nothing left to release
                redis_lock.release()

    @contextlib.contextmanager
    def batch(self, lock=True):
        """
        Yield a private copy of the local DB; on exit publish it as an Overwrite.

        Overwrite means last write wins, with no version check. Holds the Write lock for the
        whole block and raises Conflict at once if another client holds it; lock=False skips it
        (only safe with a single writer). If the block raises, the copy is thrown away.
        query() and sync() keep working meanwhile, since the local lock is held only to copy.
        """
        with self._write_lock(lock):
            db, base = self._local.fork()
            try:
                with db:  # commits on success, rolls back if the block raises
                    yield db
                new = self._store.overwrite(self._local.freeze(db), base)
            except BaseException:
                db.close()
                raise
            self._local.install(db, new)

    async def awrite(self, fn, lock=True):
        """
        write() without blocking the event loop. fn may be a plain function or an `async def`.

        An async fn runs on the caller's event loop (so it can await your async clients) while
        the Redis I/O stays in a worker thread.
        """
        if inspect.iscoroutinefunction(fn):
            loop, afn = asyncio.get_running_loop(), fn

            def fn(db):
                # write() runs in a worker thread; hand the coroutine back to the loop and wait.
                return asyncio.run_coroutine_threadsafe(afn(db), loop).result()

        await asyncio.to_thread(self.write, fn, lock)

    @contextlib.asynccontextmanager
    async def abatch(self, lock=True):
        """
        batch() for async code: `async with db.abatch() as c:` lets the block await.

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

    def sync(self):
        """
        Load the snapshot if Redis holds a different version than ours."""
        seen = self.version
        loaded = self._store.load(seen)
        # False: a local write landed meanwhile; the next sync rechecks.
        if loaded is None or self._local.load(*loaded, seen):
            self._stats.synced()

    def close(self):
        if self._listener:
            self._listener.stop(self.poll + 5)
            self._stats.forget()
        self._store.close()
        self._local.close()
