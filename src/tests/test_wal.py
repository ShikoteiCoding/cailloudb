import zlib

import pytest

from cailloudb import InMemoryStore, Wal, WriteBatch
from cailloudb.settings import (
    STRUCT_WAL_CRC,
    STRUCT_WAL_LEN,
    STRUCT_WAL_PAYLOAD_LEN,
    STRUCT_WAL_SEQ,
    WAL_BATCH_KIND_FIRST,
    WAL_BATCH_KIND_FULL,
    WAL_BATCH_KIND_LAST,
    WAL_BATCH_KIND_MIDDLE,
    WAL_SINGLE_KIND,
    Settings,
)

__all__ = [
    "test_recover_empty_wal",
    "test_append_put_then_recover",
    "test_append_none_value_then_recover",
    "test_append_delete_then_recover",
    "test_append_writes_length_prefixed_record",
    "test_append_preserves_order",
    "test_clear_drops_logged_records",
    "test_append_after_clear",
    "test_reopen_opens_a_new_file",
    "test_store_put_and_delete_append_to_wal",
    "test_store_write_appends_one_batch_record",
    "test_store_mixes_batch_and_single_ops_in_the_wal",
    "test_recover_batch_assigns_one_sequence_per_operation",
    "test_append_large_batch_fragments_across_blocks",
    "test_recover_replays_into_empty_store",
    "test_recover_returns_records_before_a_short_tail",
    "test_recover_returns_records_before_a_partial_header",
    "test_recover_rejects_unknown_record_kind",
    "test_checksum_mismatch_before_end_of_file_raises",
    "test_checksum_mismatch_on_the_last_record_stops",
    "test_append_rejects_payload_longer_than_uint16",
]

WAL_BLOCK_SIZE = Settings().wal_block_size


@pytest.mark.asyncio
async def test_recover_empty_wal(tmp_path):
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)

    records = [record async for record in wal.recover()]
    assert records == []


@pytest.mark.asyncio
async def test_append_put_then_recover(tmp_path):
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"a", 0, b"1")

    records = [record async for record in wal.recover()]
    assert records == [(b"a", 0, b"1")]


@pytest.mark.asyncio
async def test_append_none_value_then_recover(tmp_path):
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"a", 0, None)

    records = [record async for record in wal.recover()]
    assert records == [(b"a", 0, b"")]


@pytest.mark.asyncio
async def test_append_delete_then_recover(tmp_path):
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"a", 0, b"")

    records = [record async for record in wal.recover()]
    assert records == [(b"a", 0, b"")]


@pytest.mark.asyncio
async def test_append_writes_length_prefixed_record(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"ab", 0, b"xyz")

    payload = (
        STRUCT_WAL_SEQ.pack(0)
        + STRUCT_WAL_LEN.pack(2)
        + STRUCT_WAL_LEN.pack(3)
        + b"ab"
        + b"xyz"
    )
    checksum = zlib.crc32(bytes([WAL_SINGLE_KIND]) + payload) & 0xFFFFFFFF
    assert wal._path.name == "wal_000001.log"
    assert wal._path.read_bytes() == (
        STRUCT_WAL_CRC.pack(checksum)
        + STRUCT_WAL_PAYLOAD_LEN.pack(len(payload))
        + bytes([WAL_SINGLE_KIND])
        + payload
    )


@pytest.mark.asyncio
async def test_append_preserves_order(tmp_path):
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"a", 0, b"1")
    wal.append(b"b", 1, b"2")
    wal.append(b"a", 2, b"")
    wal.append(b"c", 3, b"3")

    records = [record async for record in wal.recover()]
    assert records == [
        (b"a", 0, b"1"),
        (b"b", 1, b"2"),
        (b"a", 2, b""),
        (b"c", 3, b"3"),
    ]


@pytest.mark.asyncio
async def test_clear_drops_logged_records(tmp_path):
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"a", 0, b"1")
    sealed = wal._path
    assert sealed.name == "wal_000001.log"

    wal.roll()

    assert wal._path.name == "wal_000002.log"
    assert sealed.read_bytes()
    records = [record async for record in wal.recover()]
    assert records == []


@pytest.mark.asyncio
async def test_reopen_opens_a_new_file(tmp_path):
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"a", 0, b"1")
    previous = wal._path

    reopened = Wal(tmp_path / "wal", Settings().wal_block_size)

    assert previous.name == "wal_000001.log"
    assert previous.read_bytes()
    assert reopened._path.name == "wal_000002.log"
    records = [record async for record in reopened.recover()]
    assert records == []


@pytest.mark.asyncio
async def test_append_after_clear(tmp_path):
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"a", 0, b"1")
    wal.roll()
    wal.append(b"b", 1, b"2")

    records = [record async for record in wal.recover()]
    assert records == [(b"b", 1, b"2")]


@pytest.mark.asyncio
async def test_store_put_and_delete_append_to_wal(tmp_path):
    store = InMemoryStore(tmp_path / "wal")

    await store.put(b"a", b"1")
    await store.put(b"b", b"2")
    await store.delete(b"a")

    records = [record async for record in store._InMemoryStore__tree._wal.recover()]  # type: ignore
    assert records == [
        (b"a", 0, b"1"),
        (b"b", 1, b"2"),
        (b"a", 2, b""),
    ]


@pytest.mark.asyncio
async def test_store_write_appends_one_batch_record(tmp_path):
    store = InMemoryStore(tmp_path / "wal")
    batch = WriteBatch()
    batch.put(b"a", b"1")
    batch.put(b"b", b"2")
    batch.delete(b"a")
    await store.write(batch)

    records = [record async for record in store._InMemoryStore__tree._wal.recover()]  # type: ignore
    assert records == [
        (b"a", 0, b"1"),
        (b"b", 1, b"2"),
        (b"a", 2, b""),
    ]


@pytest.mark.asyncio
async def test_store_mixes_batch_and_single_ops_in_the_wal(tmp_path):
    store = InMemoryStore(tmp_path / "wal")

    await store.put(b"a", b"1")
    batch = WriteBatch()
    batch.put(b"b", b"2")
    batch.delete(b"a")
    await store.write(batch)
    await store.put(b"c", b"3")

    records = [record async for record in store._InMemoryStore__tree._wal.recover()]  # type: ignore
    assert records == [
        (b"a", 0, b"1"),
        (b"b", 1, b"2"),
        (b"a", 2, b""),
        (b"c", 3, b"3"),
    ]


@pytest.mark.asyncio
async def test_recover_batch_assigns_one_sequence_per_operation(tmp_path):
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    batch = WriteBatch()
    batch.put(b"b", b"lyon")
    batch.put(b"c", b"paris")
    batch.sync_header(2)
    wal.append(batch, 2)

    records = [record async for record in wal.recover()]
    assert records == [
        (b"b", 2, b"lyon"),
        (b"c", 3, b"paris"),
    ]


@pytest.mark.asyncio
async def test_append_large_batch_fragments_across_blocks(tmp_path):
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    count = 5000
    batch = WriteBatch()
    for i in range(count):
        batch.put(i.to_bytes(4, "big"), b"v")
    batch.sync_header(1)
    wal.append(batch, 0)

    data = wal._path.read_bytes()
    block = WAL_BLOCK_SIZE
    assert data[6] == WAL_BATCH_KIND_FIRST
    assert data[block + 6] == WAL_BATCH_KIND_MIDDLE
    assert data[block * 2 + 6] == WAL_BATCH_KIND_LAST

    records = [record async for record in wal.recover()]
    assert records == [(i.to_bytes(4, "big"), i + 1, b"v") for i in range(count)]


@pytest.mark.asyncio
async def test_recover_replays_into_empty_store(tmp_path):
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"a", 0, b"1")
    wal.append(b"b", 1, b"2")
    wal.append(b"a", 2, b"")

    store = InMemoryStore(tmp_path / "wal")
    async for key, _, val in wal.recover():
        if val:
            await store.put(key, val)
        else:
            await store.delete(key)

    assert await store.get(b"b") == b"2"
    assert await store.get(b"a") is None


@pytest.mark.asyncio
async def test_recover_returns_records_before_a_short_tail(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"a", 1, b"1")
    wal.append(b"b", 2, b"2")

    path = wal._path
    path.write_bytes(path.read_bytes()[:-8])

    records = [record async for record in wal.recover()]
    assert records == [(b"a", 1, b"1")]


@pytest.mark.asyncio
async def test_recover_returns_records_before_a_partial_header(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"a", 1, b"1")

    path = wal._path
    path.write_bytes(path.read_bytes() + b"\x01\x02\x03")

    records = [record async for record in wal.recover()]
    assert records == [(b"a", 1, b"1")]


@pytest.mark.asyncio
async def test_recover_rejects_unknown_record_kind(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)

    kind = b"\xff"
    payload = b"\x00" * 24
    record_data = kind + payload
    checksum = zlib.crc32(record_data) & 0xFFFFFFFF
    frame = (
        STRUCT_WAL_CRC.pack(checksum)
        + STRUCT_WAL_PAYLOAD_LEN.pack(len(payload))
        + record_data
    )
    wal._path.write_bytes(frame)

    with pytest.raises(ValueError, match="Unknown `kind` from WAL file"):
        _ = [record async for record in wal.recover()]


@pytest.mark.asyncio
async def test_checksum_mismatch_before_end_of_file_raises(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"a", 1, b"1")
    log = wal._path
    middle = log.stat().st_size
    wal.append(b"b", 2, b"2")
    wal.append(b"c", 3, b"3")

    data = bytearray(log.read_bytes())
    data[middle] ^= 0xFF
    log.write_bytes(data)

    with pytest.raises(ValueError, match="wal checksum mismatch"):
        _ = [record async for record in wal.recover()]


@pytest.mark.asyncio
async def test_checksum_mismatch_on_the_last_record_stops(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"a", 1, b"1")
    log = wal._path
    last = log.stat().st_size
    wal.append(b"b", 2, b"2")

    data = bytearray(log.read_bytes())
    data[last] ^= 0xFF
    log.write_bytes(data)

    records = [record async for record in wal.recover()]
    assert records == [(b"a", 1, b"1")]


@pytest.mark.asyncio
async def test_append_rejects_payload_longer_than_uint16(tmp_path):
    wal = Wal(tmp_path / "wal", Settings().wal_block_size)
    wal.append(b"a", 1, b"1")

    with pytest.raises(ValueError, match="wal single record exceeds block"):
        wal.append(b"k", 2, b"x" * 65519)

    records = [record async for record in wal.recover()]
    assert records == [(b"a", 1, b"1")]
