import struct
import zlib

import pytest

from cailloudb import InMemoryStore, Wal, WriteBatch


_CRC = struct.Struct(">I")
_PLEN = struct.Struct(">H")
_SEQ = struct.Struct(">Q")
_TS = struct.Struct(">Q")
_LEN = struct.Struct(">I")
_TIMESTAMP = 1_758_950_001


@pytest.mark.asyncio
async def test_recover_empty_wal(tmp_path):
    wal = Wal(tmp_path / "wal")

    records = [record async for record in wal.recover()]
    assert records == []


@pytest.mark.asyncio
async def test_append_put_then_recover(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", b"1", timestamp=_TIMESTAMP)

    records = [record async for record in wal.recover()]
    assert records == [(0, b"a", b"1", _TIMESTAMP)]


@pytest.mark.asyncio
async def test_append_delete_then_recover(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", b"", timestamp=_TIMESTAMP)

    records = [record async for record in wal.recover()]
    assert records == [(0, b"a", b"", _TIMESTAMP)]


@pytest.mark.asyncio
async def test_append_writes_length_prefixed_record(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(path)
    await wal.append(b"ab", b"xyz", timestamp=_TIMESTAMP)

    payload = (
        bytes([0])
        + _SEQ.pack(0)
        + _TS.pack(_TIMESTAMP)
        + _LEN.pack(2)
        + _LEN.pack(3)
        + b"ab"
        + b"xyz"
    )
    checksum = zlib.crc32(payload) & 0xFFFFFFFF
    assert path.read_bytes() == _CRC.pack(checksum) + _PLEN.pack(len(payload)) + payload


@pytest.mark.asyncio
async def test_append_preserves_order(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", b"1", timestamp=_TIMESTAMP)
    await wal.append(b"b", b"2", timestamp=_TIMESTAMP)
    await wal.append(b"a", b"", timestamp=_TIMESTAMP)
    await wal.append(b"c", b"3", timestamp=_TIMESTAMP)

    records = [record async for record in wal.recover()]
    assert records == [
        (0, b"a", b"1", _TIMESTAMP),
        (0, b"b", b"2", _TIMESTAMP),
        (0, b"a", b"", _TIMESTAMP),
        (0, b"c", b"3", _TIMESTAMP),
    ]


@pytest.mark.asyncio
async def test_clear_drops_logged_records(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", b"1")

    await wal.clear()

    records = [record async for record in wal.recover()]
    assert records == []


@pytest.mark.asyncio
async def test_append_after_clear(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", b"1")
    await wal.clear()
    await wal.append(b"b", b"2", timestamp=_TIMESTAMP)

    records = [record async for record in wal.recover()]
    assert records == [(0, b"b", b"2", _TIMESTAMP)]


@pytest.mark.asyncio
async def test_store_put_and_delete_append_to_wal(monkeypatch):
    monkeypatch.setattr("store.time.time", lambda: _TIMESTAMP)
    store = InMemoryStore()

    await store.put(b"a", b"1")
    await store.put(b"b", b"2")
    await store.delete(b"a")

    records = [record async for record in store._wal.recover()]
    assert records == [
        (1, b"a", b"1", _TIMESTAMP),
        (2, b"b", b"2", _TIMESTAMP),
        (3, b"a", b"", _TIMESTAMP),
    ]


@pytest.mark.asyncio
async def test_store_write_appends_each_operation(monkeypatch):
    monkeypatch.setattr("store.time.time", lambda: _TIMESTAMP)
    store = InMemoryStore()
    batch = WriteBatch()
    batch.put(b"a", b"1")
    batch.put(b"b", b"2")
    batch.delete(b"a")

    await store.write(batch)

    records = [record async for record in store._wal.recover()]
    assert records == [
        (1, b"a", b"1", _TIMESTAMP),
        (2, b"b", b"2", _TIMESTAMP),
        (3, b"a", b"", _TIMESTAMP),
    ]


@pytest.mark.asyncio
async def test_recover_replays_into_empty_store(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", b"1")
    await wal.append(b"b", b"2")
    await wal.append(b"a", b"")

    store = InMemoryStore()
    async for _, key, val, _timestamp in wal.recover():
        if val:
            await store.put(key, val)
        else:
            await store.delete(key)

    assert await store.get(b"b") == b"2"
    with pytest.raises(KeyError):
        await store.get(b"a")


@pytest.mark.asyncio
async def test_recover_returns_records_before_a_short_tail(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(path)
    await wal.append(b"a", b"1", seq=1, timestamp=_TIMESTAMP)
    await wal.append(b"b", b"2", seq=2, timestamp=_TIMESTAMP)

    path.write_bytes(path.read_bytes()[:-8])

    records = [record async for record in wal.recover()]
    assert records == [(1, b"a", b"1", _TIMESTAMP)]


@pytest.mark.asyncio
async def test_recover_returns_records_before_a_partial_header(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(path)
    await wal.append(b"a", b"1", seq=1, timestamp=_TIMESTAMP)

    path.write_bytes(path.read_bytes() + b"\x01\x02\x03")

    records = [record async for record in wal.recover()]
    assert records == [(1, b"a", b"1", _TIMESTAMP)]


@pytest.mark.asyncio
async def test_checksum_mismatch_before_end_of_file_raises(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(path)
    await wal.append(b"a", b"1", seq=1)
    middle = path.stat().st_size
    await wal.append(b"b", b"2", seq=2)
    await wal.append(b"c", b"3", seq=3)

    data = bytearray(path.read_bytes())
    data[middle] ^= 0xFF
    path.write_bytes(data)

    with pytest.raises(ValueError, match="wal checksum mismatch"):
        _ = [record async for record in wal.recover()]


@pytest.mark.asyncio
async def test_checksum_mismatch_on_the_last_record_stops(tmp_path):
    path = tmp_path / "wal"
    wal = Wal(path)
    await wal.append(b"a", b"1", seq=1, timestamp=_TIMESTAMP)
    last = path.stat().st_size
    await wal.append(b"b", b"2", seq=2, timestamp=_TIMESTAMP)

    data = bytearray(path.read_bytes())
    data[last] ^= 0xFF
    path.write_bytes(data)

    records = [record async for record in wal.recover()]
    assert records == [(1, b"a", b"1", _TIMESTAMP)]


@pytest.mark.asyncio
async def test_append_rejects_payload_longer_than_uint16(tmp_path):
    wal = Wal(tmp_path / "wal")
    await wal.append(b"a", b"1", seq=1, timestamp=_TIMESTAMP)

    # kind + seq + lengths + 1-byte key + value exceeds the 2-byte payload length.
    with pytest.raises(struct.error):
        await wal.append(b"k", b"x" * 65519, seq=2)

    records = [record async for record in wal.recover()]
    assert records == [(1, b"a", b"1", _TIMESTAMP)]
