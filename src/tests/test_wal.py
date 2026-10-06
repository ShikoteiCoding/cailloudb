import struct
import zlib

import pytest

from cailloudb import InMemoryStore, Wal, WriteBatch

_CRC = struct.Struct(">I")
_PLEN = struct.Struct(">H")
_SEQ = struct.Struct(">Q")
_LEN = struct.Struct(">I")


@pytest.mark.asyncio
async def test_recover_empty_wal(tmp_path):
    wal = Wal(tmp_path / "wal")

    records = [record async for record in wal.recover()]
    assert records == []


@pytest.mark.asyncio
async def test_append_put_then_recover(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", 0, b"1")

    records = [record async for record in wal.recover()]
    assert records == [(b"a", 0, b"1")]


@pytest.mark.asyncio
async def test_append_delete_then_recover(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", 0, b"")

    records = [record async for record in wal.recover()]
    assert records == [(b"a", 0, b"")]


@pytest.mark.asyncio
async def test_append_writes_length_prefixed_record(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(path)
    await wal.append(b"ab", 0, b"xyz")

    payload = _SEQ.pack(0) + _LEN.pack(2) + _LEN.pack(3) + b"ab" + b"xyz"
    checksum = zlib.crc32(bytes([Wal._SINGLE_KIND]) + payload) & 0xFFFFFFFF
    assert path.read_bytes() == (
        _CRC.pack(checksum)
        + _PLEN.pack(len(payload))
        + bytes([Wal._SINGLE_KIND])
        + payload
    )


@pytest.mark.asyncio
async def test_append_preserves_order(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", 0, b"1")
    await wal.append(b"b", 1, b"2")
    await wal.append(b"a", 2, b"")
    await wal.append(b"c", 3, b"3")

    records = [record async for record in wal.recover()]
    assert records == [
        (b"a", 0, b"1"),
        (b"b", 1, b"2"),
        (b"a", 2, b""),
        (b"c", 3, b"3"),
    ]


@pytest.mark.asyncio
async def test_clear_drops_logged_records(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", 0, b"1")

    await wal.clear()

    records = [record async for record in wal.recover()]
    assert records == []


@pytest.mark.asyncio
async def test_append_after_clear(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", 0, b"1")
    await wal.clear()
    await wal.append(b"b", 1, b"2")

    records = [record async for record in wal.recover()]
    assert records == [(b"b", 1, b"2")]


@pytest.mark.asyncio
async def test_store_put_and_delete_append_to_wal(tmp_path):
    store = InMemoryStore(tmp_path / "wal")

    await store.put(b"a", b"1")
    await store.put(b"b", b"2")
    await store.delete(b"a")

    records = [record async for record in store._wal.recover()]
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

    data = store._wal._path.read_bytes()
    (payload_len,) = _PLEN.unpack_from(data, 4)
    assert len(data) == 7 + payload_len
    assert data[6] == Wal._BATCH_KIND_FULL

    records = [record async for record in store._wal.recover()]
    assert records == [
        (b"a", 0, b"1"),
        (b"b", 1, b"2"),
        (b"a", 2, b""),
    ]


@pytest.mark.asyncio
async def test_recover_batch_assigns_one_sequence_per_operation(tmp_path):
    wal = Wal(tmp_path / "wal")
    batch = WriteBatch()
    batch.put(b"b", b"lyon")
    batch.put(b"c", b"paris")
    batch.sync_header(2)
    await wal.append(batch, 2)

    records = [record async for record in wal.recover()]
    assert records == [
        (b"b", 2, b"lyon"),
        (b"c", 3, b"paris"),
    ]


@pytest.mark.asyncio
async def test_append_large_batch_fragments_across_blocks(tmp_path):
    wal = Wal(tmp_path / "wal")
    count = 5000
    batch = WriteBatch()
    for i in range(count):
        batch.put(i.to_bytes(4, "big"), b"v")
    batch.sync_header(1)
    await wal.append(batch, 0)

    data = wal._path.read_bytes()
    block = Wal._BLOCK_SIZE
    assert data[6] == Wal._BATCH_KIND_FIRST
    assert data[block + 6] == Wal._BATCH_KIND_MIDDLE
    assert data[block * 2 + 6] == Wal._BATCH_KIND_LAST

    records = [record async for record in wal.recover()]
    assert records == [(i.to_bytes(4, "big"), i + 1, b"v") for i in range(count)]


@pytest.mark.asyncio
async def test_recover_replays_into_empty_store(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", 0, b"1")
    await wal.append(b"b", 1, b"2")
    await wal.append(b"a", 2, b"")

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
    wal = Wal(path)
    await wal.append(b"a", 1, b"1")
    await wal.append(b"b", 2, b"2")

    path.write_bytes(path.read_bytes()[:-8])

    records = [record async for record in wal.recover()]
    assert records == [(b"a", 1, b"1")]


@pytest.mark.asyncio
async def test_recover_returns_records_before_a_partial_header(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(path)
    await wal.append(b"a", 1, b"1")

    path.write_bytes(path.read_bytes() + b"\x01\x02\x03")

    records = [record async for record in wal.recover()]
    assert records == [(b"a", 1, b"1")]


@pytest.mark.asyncio
async def test_checksum_mismatch_before_end_of_file_raises(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(path)
    await wal.append(b"a", 1, b"1")
    middle = path.stat().st_size
    await wal.append(b"b", 2, b"2")
    await wal.append(b"c", 3, b"3")

    data = bytearray(path.read_bytes())
    data[middle] ^= 0xFF
    path.write_bytes(data)

    with pytest.raises(ValueError, match="wal checksum mismatch"):
        _ = [record async for record in wal.recover()]


@pytest.mark.asyncio
async def test_checksum_mismatch_on_the_last_record_stops(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(path)
    await wal.append(b"a", 1, b"1")
    last = path.stat().st_size
    await wal.append(b"b", 2, b"2")

    data = bytearray(path.read_bytes())
    data[last] ^= 0xFF
    path.write_bytes(data)

    records = [record async for record in wal.recover()]
    assert records == [(b"a", 1, b"1")]


@pytest.mark.asyncio
async def test_append_rejects_payload_longer_than_uint16(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", 1, b"1")

    with pytest.raises(ValueError, match="wal single record exceeds block"):
        await wal.append(b"k", 2, b"x" * 65519)

    records = [record async for record in wal.recover()]
    assert records == [(b"a", 1, b"1")]
