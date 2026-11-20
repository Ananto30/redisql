# write() also overwrites

Supersedes the compare-and-set part of ADR-0001. `write()` now publishes like a Batch: a plain Overwrite (last write wins), with `fn` running once on the latest Local copy. The Write lock already serializes writers, so retrying on a stale Version only guards writers that bypass the lock (`lock=False`) or outlive its TTL. We accepted that gap for one publish path and no Lua script. `retries` is removed, and `Conflict` now means only that another client holds the Write lock.

## Considered Options

- Keeping compare-and-set in `write()` and Overwrite in `batch()`: rejected because it needs two publish paths in the Snapshot store for a case the lock already covers.
