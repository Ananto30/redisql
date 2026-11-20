import logging
import uuid
import zlib

log = logging.getLogger(__name__)


class SnapshotStore:
    """
    The Snapshot in Redis: its keys, compression, size limit and Version tokens.

    Callers pass and get raw sqlite bytes. Versions are random tokens, not counters: a Redis
    flush or failover can't make an old number look current again.
    """

    def __init__(self, redis, name, max_size):
        self.redis, self.max_size = redis, max_size
        self._snap, self._ver, self._chan = f"{name}:snap", f"{name}:ver", f"{name}:chan"
        self._pubsub = None

    def subscribe(self):
        """Start receiving change notices. Do it before load(), so no update falls in the gap."""
        self._pubsub = self.redis.pubsub()
        self._pubsub.subscribe(self._chan)

    def wait(self, timeout):
        """Block until a change notice arrives or `timeout` seconds pass (redis-py resubscribes
        on reconnect)."""
        self._pubsub.get_message(timeout=timeout)

    def load(self, have):
        """
        Fetch the Snapshot if Redis holds a different Version than `have`.

        Returns None if unchanged, (raw, version) to load, or (None, b"") if Redis has no
        Snapshot (flush, failover to an empty replica).
        """
        if self._version() == have:
            return None
        data, ver = self.redis.pipeline().get(self._snap).get(self._ver).execute()
        if data is None:
            return None, b""
        return self._decompress(data), ver

    def overwrite(self, raw, base):
        """
        Replace the Snapshot with `raw` and announce it. Returns the new Version.

        Last write wins: `base` is only used to warn when it drops changes someone else made.
        """
        if len(raw) > self.max_size:  # other clients would reject it on every load
            raise ValueError(f"snapshot larger than max_size={self.max_size}")
        packed, new = zlib.compress(raw), uuid.uuid4().hex.encode()
        if self._version() != base:
            log.warning("overwrite drops changes another client made since it started")
        self.redis.pipeline().set(self._snap, packed).set(self._ver, new).publish(
            self._chan, new
        ).execute()
        return new

    def close(self):
        if self._pubsub:
            self._pubsub.close()

    def _version(self):
        return self.redis.get(self._ver) or b""

    def _decompress(self, data):
        d = zlib.decompressobj()
        out = d.decompress(data, self.max_size)
        if d.unconsumed_tail:
            raise ValueError(f"snapshot larger than max_size={self.max_size}")
        return out
