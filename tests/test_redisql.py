import json
import socket
import sqlite3
import subprocess
import threading
import time

import pytest
import redis

from redisql import Conflict, RediSQL
from redisql._listener import Listener
from redisql._local import LocalCopy


@pytest.fixture(scope="session")
def redis_server():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    proc = subprocess.Popen(
        ["redis-server", "--port", str(port), "--save", "", "--appendonly", "no"],
        stdout=subprocess.DEVNULL,
    )
    client = redis.Redis(port=port)
    for _ in range(50):
        try:
            client.ping()
            break
        except redis.ConnectionError:
            time.sleep(0.1)
    yield port
    proc.terminate()


@pytest.fixture
def make_client(redis_server, request):
    r = redis.Redis(port=redis_server)
    r.flushall()
    clients = []

    def make(listen=True, **kw):
        c = RediSQL(
            redis.Redis(
                port=redis_server,
            ),
            "t",
            poll=0.2,
            **kw,
        )
        c.start(listen=listen)
        clients.append(c)
        return c

    yield make
    for c in clients:
        c.close()


def wait_for(fn, timeout=3):
    end = time.time() + timeout
    while time.time() < end:
        if v := fn():
            return v
        time.sleep(0.02)
    return fn()


def test_write_on_a_reaches_b_via_pubsub(make_client):
    a, b = make_client(), make_client()
    a.write(lambda c: (c.execute("create table t(x)"), c.execute("insert into t values (1)")))
    assert wait_for(lambda: b.query("select x from t") if _has_table(b) else None) == [(1,)]


def _has_table(c):
    return bool(c.query("select 1 from sqlite_master where name='t'"))


def test_stats_tracks_version_size_and_sync(make_client):
    a, b = make_client(), make_client(listen=False)
    s = a.stats()
    assert s["listening"] and s["tables"] == [] and s["changed_ago"] is None
    a.write(lambda c: c.execute("create table t(x)"))
    s = a.stats()
    assert s["tables"] == ["t"] and s["size_bytes"] > 0 and s["version"] == a.version.decode()
    assert s["changed_ago"] < 3 and s["synced_ago"] < 3 and s["last_error"] is None
    assert not b.stats()["listening"]
    b.sync()
    assert b.stats()["version"] == s["version"]


def test_fleet_stats_lists_listening_clients_and_drops_closed_ones(make_client):
    a, b = make_client(), make_client()
    make_client(listen=False)
    a.write(lambda c: c.execute("create table t(x)"))
    assert wait_for(lambda: a.fleet_stats()["total"]["versions"] == [a.version.decode()])
    f = a.fleet_stats()
    assert f["total"]["workers"] == 2 and f["total"]["in_sync"]
    assert f["total"]["size_bytes"] == 2 * a.stats()["size_bytes"]
    assert all(w["pid"] == a.stats()["pid"] for w in f["workers"].values())
    b.close()
    assert wait_for(lambda: a.fleet_stats()["total"]["workers"] == 1)


def test_late_joiner_loads_existing_snapshot(make_client):
    a = make_client()
    a.write(lambda c: (c.execute("create table t(x)"), c.execute("insert into t values (7)")))
    assert make_client().query("select x from t") == [(7,)]


def test_sync_catches_up_after_missed_pubsub(make_client):
    a, b = make_client(), make_client(listen=False)
    a.write(lambda c: (c.execute("create table t(x)"), c.execute("insert into t values (3)")))
    time.sleep(0.3)
    assert not _has_table(b)
    b.sync()
    assert b.query("select x from t") == [(3,)]


def test_failed_write_rolls_back(make_client):
    a = make_client()
    a.write(lambda c: c.execute("create table t(x)"))

    def bad(c):
        c.execute("insert into t values (1)")
        raise RuntimeError

    with pytest.raises(RuntimeError):
        a.write(bad)
    assert a.query("select x from t") == []


def test_listener_converges_when_pubsub_message_is_lost(make_client):
    a, b = make_client(), make_client()
    b._store._pubsub.get_message = lambda timeout=0: time.sleep(timeout)  # b never hears a
    a.write(lambda c: (c.execute("create table t(x)"), c.execute("insert into t values (5)")))
    assert wait_for(lambda: b.query("select x from t") if _has_table(b) else None) == [(5,)]


def test_listener_survives_killed_pubsub_connection(make_client, redis_server):
    a, b = make_client(), make_client()
    time.sleep(0.2)
    redis.Redis(port=redis_server).client_kill_filter(_type="pubsub")
    time.sleep(0.5)
    a.write(lambda c: (c.execute("create table t(x)"), c.execute("insert into t values (9)")))
    assert wait_for(lambda: b.query("select x from t") if _has_table(b) else None) == [(9,)]


def _rows(c):
    return c.query("select x from t") if _has_table(c) else None


def test_clients_resync_after_redis_loses_data(make_client, redis_server):
    a, b = make_client(), make_client()
    for i in range(3):
        a.write(
            lambda c, i=i: (
                c.execute("create table if not exists t(x)")
                and c.execute("insert into t values (?)", (i,))
            )
        )
    assert wait_for(lambda: _rows(b) == [(0,), (1,), (2,)])
    redis.Redis(port=redis_server).flushall()
    a.write(lambda c: c.execute("insert into t values (99)"))
    assert wait_for(lambda: _rows(b) == [(0,), (1,), (2,), (99,)])


def test_failed_push_leaves_local_unchanged(make_client):
    a = make_client(listen=False)
    a.write(lambda c: c.execute("create table t(x)"))

    def down(*a, **kw):
        raise redis.ConnectionError

    a._store.overwrite = down
    with pytest.raises(redis.ConnectionError):
        a.write(lambda c: c.execute("insert into t values (1)"))
    assert a.query("select x from t") == []


def test_query_cannot_write(make_client):
    a = make_client()
    a.write(lambda c: c.execute("create table t(x)"))
    with pytest.raises(sqlite3.OperationalError):
        a.query("insert into t values (1)")


def test_oversized_snapshot_rejected(make_client):
    a = make_client(listen=False)
    a.write(
        lambda c: (
            c.execute("create table t(x)"),
            c.execute("insert into t values (zeroblob(100000))"),
        )
    )
    b = RediSQL(a.redis, "t", max_size=1000)
    with pytest.raises(ValueError, match="max_size"):
        b.start(listen=False)
    b.close()


def test_oversized_write_rejected_and_redis_unchanged(make_client):
    a, b = make_client(listen=False, max_size=50_000), make_client(listen=False)
    a.write(lambda c: (c.execute("create table t(x)"), c.execute("insert into t values (1)")))
    ver = a.redis.get("t:ver")
    with pytest.raises(ValueError, match="max_size"):
        a.write(lambda c: c.execute("insert into t values (zeroblob(100000))"))
    assert a.redis.get("t:ver") == ver
    assert a.query("select x from t") == [(1,)]
    b.sync()
    assert b.query("select x from t") == [(1,)]


def test_query_returns_while_write_fn_is_running(make_client):
    a = make_client(listen=False)
    a.write(lambda c: (c.execute("create table t(x)"), c.execute("insert into t values (1)")))
    started, release = threading.Event(), threading.Event()

    def slow(c):
        started.set()
        release.wait(2)
        c.execute("insert into t values (2)")

    w = threading.Thread(target=a.write, args=(slow,))
    w.start()
    assert started.wait(2)
    assert a.query("select x from t") == [(1,)]
    assert w.is_alive()  # the query didn't have to wait for the write to finish
    release.set()
    w.join(2)
    assert a.query("select x from t") == [(1,), (2,)]


def test_batch_publishes_once_on_exit(make_client):
    a, b = make_client(), make_client()
    with a.batch() as c:
        c.execute("create table t(x)")
        for i in range(3):
            c.execute("insert into t values (?)", (i,))
        assert a.redis.get("t:ver") is None
    assert wait_for(lambda: _rows(b) == [(0,), (1,), (2,)])
    assert a.query("select x from t") == [(0,), (1,), (2,)]


def test_batch_exception_throws_changes_away(make_client):
    a, b = make_client(), make_client()
    a.write(lambda c: (c.execute("create table t(x)"), c.execute("insert into t values (1)")))
    ver = a.redis.get("t:ver")
    with pytest.raises(RuntimeError), a.batch() as c:
        c.execute("insert into t values (2)")
        raise RuntimeError
    assert a.query("select x from t") == [(1,)]
    assert a.redis.get("t:ver") == ver
    assert b.query("select x from t") == [(1,)]


def test_batch_keeps_query_and_sync_running_while_open(make_client):
    a, b = make_client(), make_client()
    a.write(lambda c: c.execute("create table t(x)"))
    assert wait_for(lambda: _has_table(b))
    with b.batch() as c:
        c.execute("insert into t values (1)")
        assert b.query("select x from t") == []  # batch work isn't visible yet
        a.write(lambda c: c.execute("insert into t values (2)"), lock=False)
        # sync still loads remote writes
        assert wait_for(lambda: b.query("select x from t") == [(2,)])


def test_batch_warns_when_it_erases_another_clients_write(make_client, caplog):
    a, b = make_client(listen=False), make_client(listen=False)
    a.write(lambda c: c.execute("create table t(x)"))
    b.sync()
    with caplog.at_level("WARNING"), a.batch() as c:
        b.write(lambda c: c.execute("insert into t values (1)"), lock=False)
        c.execute("insert into t values (2)")
    assert "overwr" in caplog.text.lower()
    assert make_client().query("select x from t") == [(2,)]


def test_batch_without_interference_does_not_warn(make_client, caplog):
    a = make_client(listen=False)
    a.write(lambda c: c.execute("create table t(x)"))
    with caplog.at_level("WARNING"), a.batch() as c:
        c.execute("insert into t values (1)")
    assert not caplog.records


def test_oversized_batch_rejected(make_client):
    a = make_client(listen=False, max_size=50_000)
    a.write(lambda c: c.execute("create table t(x)"))
    ver = a.redis.get("t:ver")
    with pytest.raises(ValueError, match="max_size"), a.batch() as c:
        c.execute("insert into t values (zeroblob(100000))")
    assert a.redis.get("t:ver") == ver
    assert a.query("select x from t") == []


def test_second_client_gets_conflict_while_write_lock_is_held(make_client):
    a, b = make_client(listen=False), make_client(listen=False)
    a.write(lambda c: c.execute("create table t(x)"))
    b.sync()
    calls = []
    with a.batch():
        with pytest.raises(Conflict):
            b.write(lambda c: calls.append(1))
        with pytest.raises(Conflict), b.batch():
            calls.append(1)
    assert calls == []
    b.write(lambda c: c.execute("insert into t values (1)"))  # lock was released


def test_lock_false_skips_the_write_lock(make_client):
    a, b = make_client(listen=False), make_client(listen=False)
    a.write(lambda c: c.execute("create table t(x)"))
    b.sync()
    with a.batch():
        b.write(lambda c: c.execute("insert into t values (1)"), lock=False)
        with b.batch(lock=False) as c:
            c.execute("insert into t values (2)")


def test_locked_batch_builds_on_latest_snapshot(make_client, caplog):
    a, b = make_client(listen=False), make_client(listen=False)
    a.write(lambda c: c.execute("create table t(x)"))
    b.sync()
    a.write(lambda c: c.execute("insert into t values (1)"))  # b hasn't seen this
    with caplog.at_level("WARNING"), b.batch() as c:
        c.execute("insert into t values (2)")
    assert not caplog.records
    a.sync()
    assert sorted(a.query("select x from t")) == [(1,), (2,)]


def test_lock_released_when_write_or_batch_fails(make_client):
    a, b = make_client(listen=False), make_client(listen=False)
    a.write(lambda c: c.execute("create table t(x)"))
    b.sync()

    def bad(c):
        raise RuntimeError

    with pytest.raises(RuntimeError):
        a.write(bad)
    with pytest.raises(RuntimeError), a.batch():
        raise RuntimeError
    b.write(lambda c: c.execute("insert into t values (1)"))


def test_expired_lock_does_not_surface_lock_error(make_client):
    a = make_client(listen=False, lock_timeout=0.1)
    a.write(lambda c: c.execute("create table t(x)"))
    with a.batch() as c:
        c.execute("insert into t values (1)")
        time.sleep(0.3)
    a.write(lambda c: time.sleep(0.3))
    assert a.query("select x from t") == [(1,)]


def test_abatch_awaits_inside_block_and_publishes(make_client):
    import asyncio

    a, b = make_client(), make_client()

    async def go():
        async with a.abatch() as c:
            c.execute("create table t(x)")
            await asyncio.sleep(0.05)
            c.execute("insert into t values (1)")
        with pytest.raises(RuntimeError):
            async with a.abatch() as c:
                c.execute("insert into t values (2)")
                raise RuntimeError
        await a.awrite(lambda c: c.execute("insert into t values (3)"))

    asyncio.run(go())
    assert a.query("select x from t") == [(1,), (3,)]
    assert wait_for(lambda: _rows(b) == [(1,), (3,)])
    assert a.redis.get("t:lock") is None  # lock released from a different thread


def test_awrite_accepts_async_fn(make_client):
    import asyncio

    a = make_client()

    async def fn(c):
        await asyncio.sleep(0.01)
        c.execute("create table t(x)")
        c.execute("insert into t values (1)")

    asyncio.run(a.awrite(fn))
    assert a.query("select x from t") == [(1,)]


def test_fleet_stats_total_aggregates_worker_reports(make_client):
    a = make_client(listen=False)
    for i, (ver, err) in enumerate([("v1", None), ("v2", "boom")]):
        report = {"version": ver, "size_bytes": 10, "last_error": err}
        a.redis.set(f"t:stats:w{i}", json.dumps(report))
    total = a.fleet_stats()["total"]
    assert total["workers"] == 2 and total["versions"] == ["v1", "v2"]
    assert total["in_sync"] is False and total["size_bytes"] == 20 and total["with_errors"] == 1


def test_write_is_an_overwrite_and_runs_fn_once(make_client):
    a, b = make_client(listen=False), make_client(listen=False)
    a.write(lambda c: c.execute("create table t(x)"))
    b.sync()
    a.write(lambda c: c.execute("insert into t values (1)"))
    calls = []
    b.write(lambda c: (calls.append(1), c.execute("insert into t values (2)")), lock=False)
    a.sync()
    assert calls == [1]
    assert a.query("select x from t") == [(2,)]  # b never saw a's row: last write wins


def test_store_load_reports_unchanged_snapshot_and_empty_redis(make_client):
    a = make_client(listen=False)
    store = a._store
    assert store.load(b"") is None  # empty Redis and nothing loaded yet
    a.write(lambda c: c.execute("create table t(x)"))
    assert store.load(a.version) is None
    raw, ver = store.load(b"other")
    assert ver == a.version and raw.startswith(b"SQLite format 3")
    a.redis.delete("t:snap")
    assert store.load(b"other") == (None, b"")


def test_listener_calls_on_change_retries_after_errors_and_stops():
    calls, errors, ticks = [], [], iter(range(100))

    def wait():
        time.sleep(0.01)
        if next(ticks) == 1:
            raise RuntimeError("boom")

    lis = Listener(wait, lambda: calls.append(1), errors.append, retry=0.01)
    lis.start()
    assert wait_for(lambda: len(calls) >= 3 and errors)
    assert lis.alive
    lis.stop(2)
    assert not lis.alive
    assert [str(e) for e in errors] == ["boom"]


def test_local_copy_fork_is_isolated_and_install_swaps_it_in():
    local = LocalCopy()
    db, base = local.fork()
    db.execute("create table t(x)")
    assert local.info()["tables"] == []  # the fork doesn't touch the live data
    local.freeze(db)
    local.install(db, b"v1")
    assert base == b"" and local.version == b"v1"
    assert local.info()["tables"] == ["t"] and local.info()["changed_at"]


def test_local_copy_query_cannot_write():
    local = LocalCopy()
    with pytest.raises(sqlite3.OperationalError):
        local.query("create table t(x)")


def test_local_copy_load_refused_when_version_moved_on():
    src = LocalCopy()
    db, _ = src.fork()
    db.execute("create table t(x)")
    raw = src.freeze(db)
    local = LocalCopy()
    local.install(local.fork()[0], b"mine")
    assert local.load(raw, b"theirs", seen=b"old") is False
    assert local.info()["tables"] == [] and local.version == b"mine"
    assert local.load(raw, b"theirs", seen=b"mine") is True
    assert local.info()["tables"] == ["t"] and local.version == b"theirs"


def test_local_copy_load_none_keeps_data_and_forgets_version():
    local = LocalCopy()
    db, _ = local.fork()
    db.execute("create table t(x)")
    local.freeze(db)
    local.install(db, b"v1")
    assert local.load(None, b"", seen=b"v1") is True
    assert local.version == b"" and local.info()["tables"] == ["t"]
