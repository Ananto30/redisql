import contextlib
import json
import os
import socket
import time
import uuid


class Stats:
    """
    What this client reports about its Local copy and sync health, and the fleet's reports.

    Each listening client republishes its report to Redis every `poll` seconds.
    """

    def __init__(self, redis, name, poll, max_size, local, listening):
        self.redis, self.poll, self.max_size = redis, poll, max_size
        self._local, self._listening = local, listening
        self._synced_at = self._error = None
        self._prefix = f"{name}:stats:"
        # Unique per instance, so two clients in one process don't overwrite each other.
        self._key = f"{self._prefix}{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:4]}"
        self._published_at = 0

    def synced(self):
        self._synced_at = time.time()

    def failed(self, exc):
        self._error = repr(exc)

    def healthy(self):
        self._error = None
        self.publish()

    def report(self):
        """This client's stats as a dict. No Redis call. See RediSQL.stats()."""
        info = self._local.info()
        now = time.time()
        return {
            "version": info["version"].decode(),
            "size_bytes": info["size_bytes"],
            "max_size": self.max_size,
            "tables": info["tables"],
            "listening": self._listening(),
            "poll": self.poll,
            "synced_ago": self._synced_at and now - self._synced_at,
            "changed_ago": info["changed_at"] and now - info["changed_at"],
            "last_error": self._error,
            "host": socket.gethostname(),
            "pid": os.getpid(),
        }

    def publish(self):
        # The key expires after 3 missed polls, so a dead worker drops out of fleet().
        if time.time() - self._published_at < self.poll:
            return
        self._published_at = time.time()
        self.redis.set(self._key, json.dumps(self.report()), px=int(self.poll * 3000))

    def forget(self):
        with contextlib.suppress(Exception):  # Redis may be gone; the key expires anyway
            self.redis.delete(self._key)

    def fleet(self):
        """Every listening client's report, from Redis. See RediSQL.fleet_stats()."""
        keys = list(self.redis.scan_iter(f"{self._prefix}*"))
        raw = self.redis.mget(keys) if keys else []
        workers = {
            k.decode().removeprefix(self._prefix): json.loads(v)
            for k, v in zip(keys, raw, strict=True)
            if v
        }
        versions = sorted({w["version"] for w in workers.values()})
        total = {
            "workers": len(workers),
            "versions": versions,
            "in_sync": len(versions) <= 1,
            "size_bytes": sum(w["size_bytes"] for w in workers.values()),
            "with_errors": sum(bool(w["last_error"]) for w in workers.values()),
        }
        return {"total": total, "workers": workers}
