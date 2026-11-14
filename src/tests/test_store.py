from pathlib import Path
from typing import TypeAlias

import pytest

from cailloudb import DurableStore, InMemoryStore, WriteBatch

StoreClass: TypeAlias = type[InMemoryStore] | type[DurableStore]

STORE_CLASSES: tuple[StoreClass, ...] = (InMemoryStore, DurableStore)


def _make_store(store_cls: StoreClass, tmp_path: Path):
    return store_cls(tmp_path / store_cls.__name__)


__all__ = [
    "test_store_init_creates_directory_and_lock_file",
    "test_store_close_releases_lock_and_is_idempotent",
    "test_store_close_swallows_errors_if_lock_handle_already_closed",
    "test_store_second_instantiation_raises_and_allows_clean_close",
    "test_store_get_never_raises",
    "test_store_get_at_never_raises",
    "test_store_delete_never_raises",
    "test_store_write_batch",
    "test_store_scan",
    "test_store_scan_at",
    "test_put_rejects_none_value",
    "test_put_rejects_non_bytes_key",
]


@pytest.mark.parametrize("store_cls", STORE_CLASSES, ids=lambda cls: cls.__name__)
def test_store_init_creates_directory_and_lock_file(
    tmp_path: Path, store_cls: StoreClass
):
    store_path = tmp_path / "db" / "nested" / store_cls.__name__

    store = store_cls(store_path)

    assert store_path.exists()
    assert store_path.is_dir()
    assert (store_path / "LOCK").exists()
    assert store._lock_file is not None


@pytest.mark.parametrize("store_cls", STORE_CLASSES, ids=lambda cls: cls.__name__)
def test_store_close_releases_lock_and_is_idempotent(
    tmp_path: Path, store_cls: StoreClass
):
    store = _make_store(store_cls, tmp_path)
    lock_file = store._lock_file

    assert lock_file is not None

    store.close()

    assert lock_file.closed
    assert store._lock_file is None

    store.close()
    assert store._lock_file is None


@pytest.mark.parametrize("store_cls", STORE_CLASSES, ids=lambda cls: cls.__name__)
def test_store_close_swallows_errors_if_lock_handle_already_closed(
    tmp_path: Path, store_cls: StoreClass
):
    store = _make_store(store_cls, tmp_path)
    lock_file = store._lock_file

    assert lock_file is not None

    # Simulate a corruption
    # XXX: Cover with actual corruption scenarios
    lock_file.close()

    store.close()
    assert store._lock_file is None


@pytest.mark.parametrize("store_cls", STORE_CLASSES, ids=lambda cls: cls.__name__)
def test_store_second_instantiation_raises_and_allows_clean_close(
    tmp_path: Path, store_cls: StoreClass
):
    store_path = tmp_path / store_cls.__name__
    store = store_cls(store_path)

    with pytest.raises(RuntimeError, match="Database lock failed"):
        store_cls(store_path)

    assert store._lock_file is not None
    store.close()
    assert store._lock_file is None


@pytest.mark.asyncio
@pytest.mark.parametrize("store_cls", STORE_CLASSES, ids=lambda cls: cls.__name__)
async def test_store_get_never_raises(tmp_path: Path, store_cls: StoreClass):
    store = _make_store(store_cls, tmp_path)

    await store.put(b"test1", b"val1")
    assert await store.get(b"test1") == b"val1"

    await store.put(b"test2", b"val2")
    assert await store.get(b"test2") == b"val2"
    assert await store.get(b"test3") is None


# @pytest.mark.asyncio
# @pytest.mark.parametrize("store_cls", STORE_CLASSES, ids=lambda cls: cls.__name__)
# async def test_store_get_at_never_raises(tmp_path: Path, store_cls: StoreClass):
#     store = _make_store(store_cls, tmp_path)

#     await store.put(b"test1", b"val1")
#     assert await store.get_at(b"test1", seq_num=0) == b"val1"

#     await store.put(b"test2", b"val2")
#     assert await store.get_at(b"test2", seq_num=1) == b"val2"
#     assert await store.get_at(b"test3", seq_num=1) is None

#     assert await store.latest_sequence_number() == 2

#     assert await store.get_at(b"test2", seq_num=0) is None
#     assert await store.get_at(b"test2", seq_num=1) == b"val2"
#     assert await store.get_at(b"test3", seq_num=0) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("store_cls", STORE_CLASSES, ids=lambda cls: cls.__name__)
async def test_store_delete_never_raises(tmp_path: Path, store_cls: StoreClass):
    store = _make_store(store_cls, tmp_path)

    # Check deletion on non existent key
    await store.delete(b"test1")

    await store.put(b"test1", b"val1")
    await store.put(b"test2", b"val2")

    await store.delete(b"test1")
    assert await store.get(b"test1") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("store_cls", STORE_CLASSES, ids=lambda cls: cls.__name__)
async def test_store_write_batch(tmp_path: Path, store_cls: StoreClass):
    store = _make_store(store_cls, tmp_path)
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


@pytest.mark.asyncio
@pytest.mark.parametrize("store_cls", STORE_CLASSES, ids=lambda cls: cls.__name__)
async def test_store_scan(tmp_path: Path, store_cls: StoreClass):
    store = _make_store(store_cls, tmp_path)

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


# @pytest.mark.asyncio
# @pytest.mark.parametrize("store_cls", STORE_CLASSES, ids=lambda cls: cls.__name__)
# async def test_store_scan_at(tmp_path: Path, store_cls: StoreClass):
#     store = _make_store(store_cls, tmp_path)

#     await store.put(b"b", b"2")
#     await store.put(b"a", b"1")
#     await store.put(b"c", b"3")

#     items = [item async for item in store.scan_at(seq_num=2)]
#     assert items == [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]

#     items = [item async for item in store.scan_at(b"b", seq_num=2)]
#     assert items == [(b"b", b"2"), (b"c", b"3")]

#     items = [item async for item in store.scan_at(b"b", b"c", seq_num=2)]
#     assert items == [(b"b", b"2")]

#     await store.delete(b"b")

#     items = [item async for item in store.scan_at(seq_num=3)]
#     assert items == [(b"a", b"1"), (b"c", b"3")]


@pytest.mark.asyncio
@pytest.mark.parametrize("store_cls", STORE_CLASSES, ids=lambda cls: cls.__name__)
async def test_put_rejects_none_value(tmp_path: Path, store_cls: StoreClass):
    store = _make_store(store_cls, tmp_path)

    with pytest.raises(ValueError):
        await store.put(b"a", None)  # type: ignore

    records = [record async for record in store._tree._wal.recover()]  # type: ignore
    assert records == []


@pytest.mark.asyncio
@pytest.mark.parametrize("store_cls", STORE_CLASSES, ids=lambda cls: cls.__name__)
async def test_put_rejects_non_bytes_key(tmp_path: Path, store_cls: StoreClass):
    store = _make_store(store_cls, tmp_path)

    with pytest.raises(KeyError, match="invalid for key"):
        await store.put("a", b"1")  # type: ignore[arg-type]

    records = [record async for record in store._tree._wal.recover()]  # type: ignore
    assert records == []
