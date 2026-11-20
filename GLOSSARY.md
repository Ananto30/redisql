# redisql

Clients that share one sqlite database through Redis, each holding a full local copy.

## Language

**Snapshot**:
The whole database as stored in Redis, identified by a Version.
_Avoid_: dump, image, backup

**Version**:
A random token naming one Snapshot; a different token means different data, never "newer".
_Avoid_: revision, counter, sequence number

**Local copy**:
A client's in-memory database, loaded from a Snapshot.
_Avoid_: replica, cache

**Overwrite**:
Replacing the Snapshot with a client's Local copy regardless of what Redis holds (last write wins).
_Avoid_: snapshot (as a verb), force push

**Batch**:
A group of changes a client makes in one go and publishes as a single Overwrite.
_Avoid_: bulk write, local write

**Snapshot store**:
The one place that reads and replaces the Snapshot in Redis and announces each new Version.
_Avoid_: redis layer, persistence

**Stats**:
What a client reports about its Local copy and sync health; the fleet's Stats are every listening client's report read from Redis.
_Avoid_: metrics, health

**Write lock**:
A lock held in Redis that lets one client at a time change the Snapshot.
_Avoid_: mutex, distributed lock
