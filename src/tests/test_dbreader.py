import pytest

from cailloudb import Db, ObjectStore


@pytest.mark.asyncio
async def test_dbreader_get():
    store = ObjectStore.resolve(":memory:")
    db = Db(store)
    reader = db.reader()

    await db.put(b"k", b"v")
    assert await reader.get(b"k") == b"v"

    with pytest.raises(KeyError):
        await reader.get(b"missing")


@pytest.mark.asyncio
async def test_dbreader_exists():
    store = ObjectStore.resolve(":memory:")
    db = Db(store)
    reader = db.reader()

    await db.put(b"k", b"v")

    assert await reader.exists(b"k")
    assert not await reader.exists(b"missing")


@pytest.mark.asyncio
async def test_dbreader_scan():
    store = ObjectStore.resolve(":memory:")
    db = Db(store)
    reader = db.reader()

    await db.put(b"b", b"2")
    await db.put(b"a", b"1")
    await db.put(b"c", b"3")

    items = [item async for item in reader.scan()]
    assert items == [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]

    items = [item async for item in reader.scan(b"b", b"c")]
    assert items == [(b"b", b"2")]


@pytest.mark.asyncio
async def test_dbreader_sees_db_writes():
    store = ObjectStore.resolve(":memory:")
    db = Db(store)
    reader = db.reader()

    assert not await reader.exists(b"k")

    await db.put(b"k", b"v1")
    assert await reader.get(b"k") == b"v1"

    await db.put(b"k", b"v2")
    assert await reader.get(b"k") == b"v2"

    await db.delete(b"k")
    assert not await reader.exists(b"k")


@pytest.mark.asyncio
async def test_dbreader_latest_sequence_number():
    store = ObjectStore.resolve(":memory:")
    db = Db(store)
    reader = db.reader()

    assert await reader.latest_sequence_number() == 0

    await db.put(b"a", b"1")
    await db.put(b"b", b"2")

    assert await reader.latest_sequence_number() == 2


@pytest.mark.asyncio
async def test_db_reader_shares_store():
    store = ObjectStore.resolve(":memory:")
    db = Db(store)

    reader_a = db.reader()
    reader_b = db.reader()

    await db.put(b"k", b"v")

    assert await reader_a.get(b"k") == b"v"
    assert await reader_b.get(b"k") == b"v"
