import sqlite3
import threading
import time


class LocalCopy:
    """
    A client's in-memory database and the Version it was loaded from.

    Data changes only by install() or load(), never through query(). The lock is held only for
    quick reads and swaps, so queries aren't blocked while a caller works on a fork().
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._db = sqlite3.connect(":memory:", check_same_thread=False)
        self._db.execute("pragma query_only=1")
        self.version, self.changed_at = b"", None

    def query(self, sql, params=()):
        with self._lock:
            return self._db.execute(sql, params).fetchall()

    def fork(self):
        """Return (private writable copy, the Version it was copied from)."""
        db = sqlite3.connect(":memory:", check_same_thread=False)
        with self._lock:
            self._db.backup(db)
            return db, self.version

    def freeze(self, db):
        """Make a fork read-only, ready to be installed, and return its bytes."""
        db.execute("pragma query_only=1")
        return db.serialize()

    def install(self, db, version):
        """Replace the data with a frozen fork. The old data is closed."""
        with self._lock:
            self._db, old = db, self._db
            old.close()
            self.version, self.changed_at = version, time.time()

    def load(self, raw, version, seen):
        """
        Replace the data with a Snapshot, unless the Version moved on from `seen`.

        Returns False if it did (a local write landed meanwhile). raw=None means Redis lost the
        Snapshot: keep the data and forget the Version, so the next write re-seeds Redis.
        """
        with self._lock:
            if self.version != seen:
                return False
            if raw is None:
                self.version = b""
            else:
                self._db.deserialize(raw)
                self.version, self.changed_at = version, time.time()
            return True

    def info(self):
        with self._lock:
            pages, page_size = (
                self._db.execute(f"pragma {p}").fetchone()[0] for p in ("page_count", "page_size")
            )
            tables = [
                r[0] for r in self._db.execute("select name from sqlite_master where type='table'")
            ]
            return {
                "version": self.version,
                "size_bytes": pages * page_size,
                "tables": tables,
                "changed_at": self.changed_at,
            }

    def close(self):
        self._db.close()
