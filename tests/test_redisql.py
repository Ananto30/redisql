import socket
import subprocess
import time

import pytest
import redis

from redisql import RediSQL


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

    def make(listen=True):
        c = RediSQL(redis.Redis(port=redis_server), "t")
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
