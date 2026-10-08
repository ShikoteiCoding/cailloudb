import pytest

from cailloudb import InMemoryStore, WriteBatch

__all__ = [
    "test_in_memory_store_get_never_raises",
    "test_in_memory_store_get_at_never_raises",
    "test_in_memory_store_delete_never_raises",
    "test_store_write_batch",
    "test_in_memory_store_scan",
    "test_in_memory_store_scan_at",
    "test_put_rejects_none_value",
    "test_put_rejects_non_bytes_key",
]


@pytest.mark.asyncio
async def test_in_memory_store_get_never_raises(tmp_path):
    store = InMemoryStore(tmp_path / "wal")

    await store.put(b"test1", b"val1")
    assert await store.get(b"test1") == b"val1"

    await store.put(b"test2", b"val2")
    assert await store.get(b"test2") == b"val2"
    assert await store.get(b"test3") is None

    assert await store.latest_sequence_number() == 2


@pytest.mark.asyncio
async def test_in_memory_store_get_at_never_raises(tmp_path):
    store = InMemoryStore(tmp_path / "wal")

    await store.put(b"test1", b"val1")
    assert await store.get_at(b"test1", seq_num=0) == b"val1"

    await store.put(b"test2", b"val2")
    assert await store.get_at(b"test2", seq_num=1) == b"val2"
    assert await store.get_at(b"test3", seq_num=1) is None

    assert await store.latest_sequence_number() == 2

    assert await store.get_at(b"test2", seq_num=0) is None
    assert await store.get_at(b"test2", seq_num=1) == b"val2"
    assert await store.get_at(b"test3", seq_num=0) is None


@pytest.mark.asyncio
async def test_in_memory_store_delete_never_raises(tmp_path):
    store = InMemoryStore(tmp_path / "wal")

    # Check deletion on non existent key
    await store.delete(b"test1")

    await store.put(b"test1", b"val1")
    await store.put(b"test2", b"val2")

    await store.delete(b"test1")
    assert await store.get(b"test1") is None

    assert await store.latest_sequence_number() == 4


@pytest.mark.asyncio
async def test_store_write_batch(tmp_path):
    store = InMemoryStore(tmp_path / "wal")
    batch = WriteBatch()

    batch.put(b"test1", b"val1")
    batch.put(b"test2", b"val2")
    batch.put(b"test3", b"val3")
    batch.delete(b"test2")

    assert len(batch) == 4

    await store.write(batch)

    assert await store.get(b"test1") == b"val1"
    assert await store.get(b"test3") == b"val3"

    assert await store.get(b"test2") is None

    assert await store.latest_sequence_number() == 4


@pytest.mark.asyncio
async def test_in_memory_store_scan(tmp_path):
    store = InMemoryStore(tmp_path / "wal")

    await store.put(b"b", b"2")
    await store.put(b"a", b"1")
    await store.put(b"c", b"3")

    items = [item async for item in store.scan()]
    assert items == [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]

    items = [item async for item in store.scan(b"b")]
    assert items == [(b"b", b"2"), (b"c", b"3")]

    items = [item async for item in store.scan(b"b", b"c")]
    assert items == [(b"b", b"2")]

    await store.delete(b"b")

    items = [item async for item in store.scan()]
    assert items == [(b"a", b"1"), (b"c", b"3")]


@pytest.mark.asyncio
async def test_in_memory_store_scan_at(tmp_path):
    store = InMemoryStore(tmp_path / "wal")

    await store.put(b"b", b"2")
    await store.put(b"a", b"1")
    await store.put(b"c", b"3")

    items = [item async for item in store.scan_at(seq_num=2)]
    assert items == [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]

    items = [item async for item in store.scan_at(b"b", seq_num=2)]
    assert items == [(b"b", b"2"), (b"c", b"3")]

    items = [item async for item in store.scan_at(b"b", b"c", seq_num=2)]
    assert items == [(b"b", b"2")]

    await store.delete(b"b")

    items = [item async for item in store.scan_at(seq_num=3)]
    assert items == [(b"a", b"1"), (b"c", b"3")]


@pytest.mark.asyncio
async def test_put_rejects_none_value(tmp_path):
    store = InMemoryStore(tmp_path / "wal")

    with pytest.raises(ValueError):
        await store.put(b"a", None)  # type: ignore

    assert await store.latest_sequence_number() == 0
    records = [record async for record in store._wal.recover()]
    assert records == []


@pytest.mark.asyncio
async def test_put_rejects_non_bytes_key(tmp_path):
    store = InMemoryStore(tmp_path / "wal")

    with pytest.raises(KeyError, match="invalid for key"):
        await store.put("a", b"1")  # type: ignore[arg-type]

    assert await store.latest_sequence_number() == 0
    records = [record async for record in store._wal.recover()]
    assert records == []
