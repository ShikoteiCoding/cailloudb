import time
from pathlib import Path

import pytest

from cailloudb import DbBuilder, DurableStore, InMemoryStore
from cailloudb.settings import Settings

__all__ = [
    "test_dbreader_get",
    "test_dbreader_scan",
    "test_dbreader_sees_db_writes",
    "test_dbreader_latest_sequence_number",
    "test_db_reader_shares_store",
    "test_dbreader_recovers_flushed_sstable_on_reopen",
    "test_dbreader_scan_recovers_flushed_sstable_on_reopen",
]


def _wait_for_condition(predicate, timeout_s: float = 2.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("Timed out waiting for condition")


@pytest.mark.asyncio
async def test_dbreader_get(tmp_path: Path):
    store = InMemoryStore(tmp_path)
    db = DbBuilder(store).build()
    reader = db.reader()

    await db.put(b"k", b"v")
    assert await reader.get(b"k") == b"v"

    assert await reader.get(b"missing") is None


@pytest.mark.asyncio
async def test_dbreader_scan(tmp_path: Path):
    store = InMemoryStore(tmp_path)
    db = DbBuilder(store).build()
    reader = db.reader()

    await db.put(b"b", b"2")
    await db.put(b"a", b"1")
    await db.put(b"c", b"3")

    items = [item async for item in reader.scan()]
    assert items == [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]

    items = [item async for item in reader.scan(b"b", b"c")]
    assert items == [(b"b", b"2")]


@pytest.mark.asyncio
async def test_dbreader_sees_db_writes(tmp_path: Path):
    store = InMemoryStore(tmp_path)
    db = DbBuilder(store).build()
    reader = db.reader()

    assert await reader.get(b"k") is None

    await db.put(b"k", b"v1")
    assert await reader.get(b"k") == b"v1"

    await db.put(b"k", b"v2")
    assert await reader.get(b"k") == b"v2"

    await db.delete(b"k")
    assert await reader.get(b"k") is None


@pytest.mark.asyncio
async def test_db_reader_shares_store(tmp_path: Path):
    store = InMemoryStore(tmp_path)
    db = DbBuilder(store).build()

    reader_a = db.reader()
    reader_b = db.reader()

    await db.put(b"k", b"v")

    assert await reader_a.get(b"k") == b"v"
    assert await reader_b.get(b"k") == b"v"


@pytest.mark.asyncio
async def test_dbreader_recovers_flushed_sstable_on_reopen(tmp_path: Path):
    db_path = tmp_path / "durable-reader-get-reopen"
    flushed_value = b"x" * 128
    in_mem_value = b"tail"
    first_store = DurableStore(
        db_path,
        Settings(
            # Force one-record flush for large values; keep a small final value in memtable.
            memtable_max_bytes_size=64,
            compaction_enabled=False,
        ),
    )
    first_db = DbBuilder(first_store).build()

    await first_db.put(b"key1", flushed_value)
    await first_db.put(b"key2", flushed_value)
    await first_db.put(b"key3", flushed_value)
    await first_db.put(b"key4", flushed_value)
    _wait_for_condition(lambda: len(list(db_path.glob("*.sst"))) >= 4)
    await first_db.put(b"key5", in_mem_value)

    first_store.close()

    second_store = DurableStore(db_path)
    second_db = DbBuilder(second_store).build()
    reader = second_db.reader()

    assert await reader.get(b"key1") == flushed_value
    # assert await reader.get(b"key2") == flushed_value
    # assert await reader.get(b"key3") == flushed_value
    # assert await reader.get(b"key4") == flushed_value
    assert await reader.get(b"key5") == in_mem_value
    second_store.close()


@pytest.mark.asyncio
async def test_dbreader_scan_recovers_flushed_sstable_on_reopen(tmp_path: Path):
    db_path = tmp_path / "durable-reader-scan-reopen"
    flushed_value = b"x" * 128
    in_mem_value = b"tail"
    first_store = DurableStore(
        db_path,
        Settings(
            # Force one-record flush for large values; keep a small final value in memtable.
            memtable_max_bytes_size=64,
            compaction_enabled=False,
        ),
    )
    first_db = DbBuilder(first_store).build()

    await first_db.put(b"key1", flushed_value)
    await first_db.put(b"key2", flushed_value)
    await first_db.put(b"key3", flushed_value)
    await first_db.put(b"key4", flushed_value)
    _wait_for_condition(lambda: len(list(db_path.glob("*.sst"))) >= 4)
    await first_db.put(b"key5", in_mem_value)

    first_store.close()

    second_store = DurableStore(db_path)
    second_db = DbBuilder(second_store).build()
    reader = second_db.reader()

    assert [item async for item in reader.scan()] == [
        (b"key1", flushed_value),
        (b"key2", flushed_value),
        (b"key3", flushed_value),
        (b"key4", flushed_value),
        (b"key5", in_mem_value),
    ]
    second_store.close()
