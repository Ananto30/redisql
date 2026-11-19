import socket
import sqlite3
import subprocess
import time

import pytest
import redis

from redisql import Conflict, RediSQL


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
        c = RediSQL(redis.Redis(port=redis_server, ), "t", poll=0.2, **kw)
        c.start(listen=listen)
        clients.append(c)
        return c

    yield make
    for c in clients:
        c.close()


def wait_for(fn, timeout=3):
    end = time.time() + timeout
    while time.time() < end:
        if (v := fn()):
            return v
        time.sleep(0.02)
    return fn()


def test_write_on_a_reaches_b_via_pubsub(make_client):
    a, b = make_client(), make_client()
    a.write(lambda c: (c.execute("create table t(x)"), c.execute("insert into t values (1)")))
    assert wait_for(lambda: b.query("select x from t") if _has_table(b) else None) == [(1,)]


def _has_table(c):
    return bool(c.query("select 1 from sqlite_master where name='t'"))


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
    a.redis.publish = lambda *args: 0  # the notification never reaches anyone
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
        a.write(lambda c, i=i: c.execute("create table if not exists t(x)") and c.execute("insert into t values (?)", (i,)))
    assert wait_for(lambda: _rows(b) == [(0,), (1,), (2,)])
    redis.Redis(port=redis_server).flushall()
    a.write(lambda c: c.execute("insert into t values (99)"))
    assert wait_for(lambda: _rows(b) == [(0,), (1,), (2,), (99,)])


def test_failed_push_leaves_local_unchanged(make_client):
    a = make_client(listen=False)
    a.write(lambda c: c.execute("create table t(x)"))

    def down(*a, **kw):
        raise redis.ConnectionError

    a._push = down
    with pytest.raises(redis.ConnectionError):
        a.write(lambda c: c.execute("insert into t values (1)"))
    assert a.query("select x from t") == []


def test_stale_writer_reruns_on_latest_instead_of_clobbering(make_client):
    a, b = make_client(listen=False), make_client(listen=False)
    a.write(lambda c: c.execute("create table t(x)"))
    b.sync()
    a.write(lambda c: c.execute("insert into t values (1)"))
    b.write(lambda c: c.execute("insert into t values (2)"))  # b hasn't seen a's write
    a.sync()
    assert sorted(a.query("select x from t")) == [(1,), (2,)]


def test_conflict_raised_when_retries_exhausted(make_client):
    a = make_client(listen=False, retries=1)
    a.write(lambda c: c.execute("create table t(x)"))
    a.version = b"stale"
    a.sync = lambda: None
    with pytest.raises(Conflict):
        a.write(lambda c: c.execute("insert into t values (1)"))
    assert a.query("select x from t") == []


def test_query_cannot_write(make_client):
    a = make_client()
    a.write(lambda c: c.execute("create table t(x)"))
    with pytest.raises(sqlite3.OperationalError):
        a.query("insert into t values (1)")


def test_oversized_snapshot_rejected(make_client):
    a = make_client(listen=False)
    a.write(lambda c: (c.execute("create table t(x)"), c.execute("insert into t values (zeroblob(100000))")))
    b = RediSQL(a.redis, "t", max_size=1000)
    with pytest.raises(ValueError, match="max_size"):
        b.start(listen=False)
    b.close()



def test_local_writes_push_once_on_snapshot(make_client):
    a, b = make_client(), make_client()
    a.local(lambda c: c.execute("create table t(x)"))
    for i in range(3):
        a.local(lambda c, i=i: c.execute("insert into t values (?)", (i,)))
    assert a.redis.get("t:ver") is None
    a.snapshot()
    assert wait_for(lambda: b.version == a.version)
    assert b.query("select x from t") == [(0,), (1,), (2,)]


def test_snapshot_keeps_local_work_and_overwrites_remote(make_client):
    a, b = make_client(), make_client()
    a.local(lambda c: c.execute("create table mine(x)"))
    b.write(lambda c: c.execute("create table theirs(x)"))
    time.sleep(0.5)  # a's listener syncs meanwhile; must not drop unpushed work
    assert a.query("select name from sqlite_master") == [("mine",)]
    a.snapshot()
    assert wait_for(lambda: b.query("select name from sqlite_master") == [("mine",)])


def test_write_with_unpushed_local_work_fails_fast_on_conflict(make_client):
    a, b = make_client(listen=False), make_client()
    a.local(lambda c: c.execute("create table mine(x)"))
    b.write(lambda c: c.execute("create table theirs(x)"))
    calls = []
    with pytest.raises(Conflict):
        a.write(lambda c: calls.append(1))
    assert calls == [1]
    assert a.query("select name from sqlite_master") == [("mine",)]
