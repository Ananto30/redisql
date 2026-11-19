# Writes are serialized by a Write lock, and a Batch overwrites

Writes must come from a single writer or happen under the Write lock, which `write()` and `batch()` take by default (`lock=True`). Because the lock serializes writers, a Batch publishes as a plain Overwrite (last write wins) instead of compare-and-set: we accepted the simpler code over guarding against writers that bypass the lock (`lock=False`) or outlive its TTL. The lock is redis-py's built-in `Lock`; replace it with our own only if it proves insufficient. Lock acquisition fails fast with `Conflict` rather than waiting.

## Considered Options

- Compare-and-set on every Batch: rejected as unneeded once writers are serialized.
- Keeping `local()` + `snapshot()`: rejected because unpushed local changes silently stopped a client from syncing.
