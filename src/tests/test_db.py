import pytest

from conftest import TEST_DB
from cailloudb import ObjectStore, DbBuilder, WriteBatch


@pytest.mark.asyncio
async def test_store_get_or_raise():
    store = ObjectStore.resolve(":memory:")
    db = DbBuilder(TEST_DB, store).build()

    await db.put(b"test1", b"val1")
    assert await db.get(b"test1") == b"val1"

    await db.put(b"test2", b"val2")
    assert await db.get(b"test2") == b"val2"

    with pytest.raises(KeyError):
        await db.get(b"test3")

    assert await db.latest_sequence_number() == 2


@pytest.mark.asyncio
async def test_store_delete_or_raise():
    store = ObjectStore.resolve(":memory:")
    db = DbBuilder(TEST_DB, store).build()

    await db.put(b"test1", b"val1")
    await db.put(b"test2", b"val2")

    await db.delete(b"test1")
    with pytest.raises(KeyError):
        await db.get(b"test1")

    with pytest.raises(KeyError):
        await db.delete(b"test1")

    assert await db.latest_sequence_number() == 3


@pytest.mark.asyncio
async def test_store_exist():
    store = ObjectStore.resolve(":memory:")
    db = DbBuilder(TEST_DB, store).build()

    await db.put(b"test1", b"val1")

    assert await db.exists(b"test1")
    assert not await db.exists(b"test2")

    assert await db.latest_sequence_number() == 1


@pytest.mark.asyncio
async def test_store_write_batch():
    store = ObjectStore.resolve(":memory:")
    db = DbBuilder(TEST_DB, store).build()

    batch = WriteBatch()

    batch.put(b"test1", b"val1")
    batch.put(b"test2", b"val2")
    batch.put(b"test3", b"val3")
    batch.delete(b"test2")

    assert len(batch) == 4

    await db.write(batch)

    assert await db.get(b"test1") == b"val1"
    assert await db.get(b"test3") == b"val3"

    with pytest.raises(KeyError):
        await db.get(b"test2")

    assert await db.latest_sequence_number() == 4


@pytest.mark.asyncio
async def test_in_memory_store_scan():
    store = ObjectStore.resolve(":memory:")
    db = DbBuilder(TEST_DB, store).build()

    await db.put(b"b", b"2")
    await db.put(b"a", b"1")
    await db.put(b"c", b"3")

    items = [item async for item in db.scan()]
    assert items == [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]

    items = [item async for item in db.scan(b"b")]
    assert items == [(b"b", b"2"), (b"c", b"3")]

    items = [item async for item in db.scan(b"b", b"c")]
    assert items == [(b"b", b"2")]

    await db.delete(b"b")

    items = [item async for item in db.scan()]
    assert items == [(b"a", b"1"), (b"c", b"3")]

