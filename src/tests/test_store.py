import pytest

from cailloudb import ObjectStore, WriteBatch


@pytest.mark.asyncio
async def test_in_memory_store_get_or_raise():
    store = ObjectStore.resolve(":memory:")

    await store.put(b"test1", b"val1")
    assert await store.get(b"test1") == b"val1"

    await store.put(b"test2", b"val2")
    assert await store.get(b"test2") == b"val2"

    with pytest.raises(KeyError):
        await store.get(b"test3")

    assert await store.latest_sequence_number() == 2


@pytest.mark.asyncio
async def test_in_memory_store_delete_or_raise():
    store = ObjectStore.resolve(":memory:")

    await store.put(b"test1", b"val1")
    await store.put(b"test2", b"val2")

    await store.delete(b"test1")
    with pytest.raises(KeyError):
        await store.get(b"test1")

    with pytest.raises(KeyError):
        await store.delete(b"test1")

    assert await store.latest_sequence_number() == 3


@pytest.mark.asyncio
async def test_in_memory_store_exist():
    store = ObjectStore.resolve(":memory:")

    await store.put(b"test1", b"val1")

    assert await store.exists(b"test1")
    assert not await store.exists(b"test2")

    assert await store.latest_sequence_number() == 1


@pytest.mark.asyncio
async def test_store_write_batch():
    store = ObjectStore.resolve(":memory:")
    batch = WriteBatch()

    batch.put(b"test1", b"val1")
    batch.put(b"test2", b"val2")
    batch.put(b"test3", b"val3")
    batch.delete(b"test2")

    assert len(batch) == 4

    await store.write(batch)

    assert await store.get(b"test1") == b"val1"
    assert await store.get(b"test3") == b"val3"

    with pytest.raises(KeyError):
        await store.get(b"test2")

    assert await store.latest_sequence_number() == 4


@pytest.mark.asyncio
async def test_in_memory_store_scan():
    store = ObjectStore.resolve(":memory:")

    await store.put(b"b", b"2")
    await store.put(b"a", b"1")
    await store.put(b"c", b"3")

    items = [item async for item in store.scan()]
    assert items == [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]

    await store.delete(b"b")
    items = [item async for item in store.scan()]
    assert items == [(b"a", b"1"), (b"c", b"3")]
