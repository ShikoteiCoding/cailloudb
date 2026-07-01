import pytest

from conftest import TEST_DB
from cailloudb import ObjectStore, DbBuilder


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


@pytest.mark.asyncio
async def test_store_exist():
    store = ObjectStore.resolve(":memory:")
    db = DbBuilder(TEST_DB, store).build()

    await db.put(b"test1", b"val1")

    assert await db.exists(b"test1")
    assert not await db.exists(b"test2")
