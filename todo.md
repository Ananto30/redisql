# TODO: Write lock and Batch

Work from the grilling session of 2026-11-20. Terms are defined in [GLOSSARY.md](GLOSSARY.md); the main decision is [ADR 0001](docs/adr/0001-write-lock-and-batch-overwrite.md).

## Decisions

- Writes come from a single writer or happen under the **Write lock**. `write()` and `batch()` take it by default (`lock=True`).
- The Write lock is redis-py's built-in `redis.lock("{name}:lock", timeout=lock_timeout)`, with `lock_timeout=30` by default. Swap it for our own lock only if redis-py's proves insufficient.
- Lock acquisition fails fast. It is tried once (`blocking=False`), and if the lock is held it raises `Conflict`. We fail fast wherever we can.
- After taking the Write lock, a client syncs to the latest Snapshot before running `fn`, so its first attempt builds on current data.
- `batch()` replaces `local()` and `snapshot()`. It is a context manager that works on a private copy of the Local copy and publishes it as an **Overwrite** on exit (last write wins, no compare-and-set). If the block raises, the copy is thrown away.
- `_dirty` is removed, so a client never stops syncing.
- `write()` keeps its compare-and-set and retries. Under the lock this costs nothing, and it still catches writers that use `lock=False`.
- `batch(lock=False)` logs a warning when its Overwrite erases another client's write. The Overwrite still happens.
- `write()` and `batch()` both reject a Snapshot above `max_size` before pushing.
- The in-process `threading` lock covers only copying and swapping the DB. `fn`, compression and the push run without it, so `query()` isn't blocked during a write. (This is separate from the Write lock, which is held for the whole write.)
- Known limit, documented and not handled in code: a write that runs longer than `lock_timeout` lets a second writer in. Set `lock_timeout` above your longest write.

## Tickets

Each ticket is a vertical slice with its own tests. Work them blockers first.

### 01: Shared copy-and-push path with size check

**Blocked by:** None.

- [ ] Extract one helper that copies the Local copy, runs `fn` on the copy, checks the size against `max_size`, compresses and pushes. `write()` uses it.
- [ ] `write()` raises `ValueError` for a DB above `max_size`, and Redis is left unchanged.
- [ ] Test: an oversized `write()` is rejected and other clients keep their data.

### 02: `write()` doesn't block reads

**Blocked by:** 01.

- [ ] The `threading` lock is held only to copy the Local copy and to swap the new DB in. `fn`, compression and the push run outside it.
- [ ] The swap happens only if our Version didn't change meanwhile. If it did, run another compare-and-set retry.
- [ ] Test: `query()` returns while a slow `fn` is still running inside `write()`.

### 03: `batch()` replaces `local()` and `snapshot()`

**Blocked by:** 01.

- [ ] `with db.batch() as c:` runs on a private copy, and on exit publishes it as an Overwrite.
- [ ] If the block raises, the copy is thrown away. Local copy and Redis are both unchanged.
- [ ] `query()` keeps returning the old data, and sync keeps running while the block is open.
- [ ] Remove `local()`, `snapshot()` and `_dirty`.
- [ ] If the Version in Redis changed since the batch started, log a warning and overwrite anyway.
- [ ] Tests: data reaches the other clients, an exception throws the changes away, the warning appears when another client's write is erased.

### 04: Write lock on by default

**Blocked by:** 02, 03.

- [ ] `write(fn, lock=True)` and `batch(lock=True)` take `redis.lock(f"{name}:lock", timeout=lock_timeout)` with `blocking=False`. The constructor gains `lock_timeout=30`.
- [ ] If the lock is held, raise `Conflict` right away. Never surface `LockError` to callers.
- [ ] After acquiring the lock, sync before copying.
- [ ] Release the lock in `finally`.
- [ ] Tests: a second client gets `Conflict` while the lock is held, a locked batch doesn't drop another client's earlier write, `lock=False` skips the lock.

### 05: README matches the new API

**Blocked by:** 04.

- [ ] Replace the `local()`/`snapshot()` section with `batch()`.
- [ ] Explain the Write lock, `lock=False` (single writer only), fail-fast `Conflict` and the `lock_timeout` limit.
- [ ] Link [GLOSSARY.md](GLOSSARY.md) and [docs/adr/](docs/adr/).
