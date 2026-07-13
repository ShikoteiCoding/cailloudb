import pytest

from cailloudb import Db, DbBuilder, ObjectStore, replay_log
from cailloudb.store import LogEntry, SeqNum


@pytest.mark.asyncio
async def test_snapshot_get():
    store = ObjectStore.resolve(":memory:")
    db = Db(store)

    await db.put(b"k", b"v1")
    snap = db.snapshot()

    await db.put(b"k", b"v2")

    assert snap.seq() == 1
    assert await snap.get(b"k") == b"v1"
    assert await db.get(b"k") == b"v2"


@pytest.mark.asyncio
async def test_snapshot_exists():
    store = ObjectStore.resolve(":memory:")
    db = Db(store)
    snap = db.snapshot()

    assert not await snap.exists(b"k")

    await db.put(b"k", b"v")

    assert not await snap.exists(b"k")
    assert await db.exists(b"k")


@pytest.mark.asyncio
async def test_snapshot_delete_replay():
    store = ObjectStore.resolve(":memory:")
    db = Db(store)

    await db.put(b"k", b"v1")
    snap = db.snapshot()
    await db.delete(b"k")

    assert await snap.get(b"k") == b"v1"
    with pytest.raises(KeyError):
        await db.get(b"k")


@pytest.mark.asyncio
async def test_snapshot_reput():
    store = ObjectStore.resolve(":memory:")
    db = Db(store)

    await db.put(b"b", b"2")
    snap = db.snapshot()
    await db.delete(b"b")
    await db.put(b"b", b"9")

    assert await snap.get(b"b") == b"2"
    assert await db.get(b"b") == b"9"


@pytest.mark.asyncio
async def test_snapshot_scan():
    store = ObjectStore.resolve(":memory:")
    db = Db(store)

    await db.put(b"b", b"2")
    await db.put(b"a", b"1")
    snap = db.snapshot()
    await db.put(b"c", b"3")
    await db.delete(b"b")

    items = [item async for item in snap.scan()]
    assert items == [(b"a", b"1"), (b"b", b"2")]

    items = [item async for item in db.scan()]
    assert items == [(b"a", b"1"), (b"c", b"3")]


@pytest.mark.asyncio
async def test_replay_log():
    log: list[LogEntry] = [
        (SeqNum.with_value(1), b"a", b"1"),
        (SeqNum.with_value(2), b"b", b"2"),
        (SeqNum.with_value(3), b"a", None),
        (SeqNum.with_value(4), b"c", b"3"),
    ]

    assert replay_log(log, SeqNum.with_value(2)) == {b"a": b"1", b"b": b"2"}
    assert replay_log(log, SeqNum.with_value(3)) == {b"b": b"2"}
    assert replay_log(log, SeqNum.with_value(4)) == {b"b": b"2", b"c": b"3"}


@pytest.mark.asyncio
async def test_dbbuilder_snapshot():
    store = ObjectStore.resolve(":memory:")
    db = DbBuilder("test-db", store).build()

    await db.put(b"k", b"v")
    snap = db.snapshot()

    assert await snap.get(b"k") == b"v"
