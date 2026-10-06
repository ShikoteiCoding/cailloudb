import struct
import zlib
from pathlib import Path
from typing import AsyncIterator, Iterator

from write_batch import TOMBSTONE, WriteBatch


class Wal:
    """
    Blocks are 32 KB

    Physical record:
      [4 bytes checksum][2 bytes payload length][1 byte type][payload]

    The checksum is crc32 of the type byte plus the payload
    Fewer than 8 bytes left in a block is zero padding

    Logical records
    Single payload
      [8 bytes sequence number][4 bytes key length][4 bytes val length][key bytes][val bytes]
    Batch payload:
      [WriteBatch payload]

    One single record is one put or one delete. An empty value is a delete.
    A batch that does not fit is split into FIRST, MIDDLE, and LAST fragments.
    Recover joins those fragments, then yields one sequence per operation.
    """

    # TODO: replace 4-byte key/value lengths with a cheaper encoding
    # TODO: extend this header (for example a log number), same append/recover/clear

    _BLOCK_SIZE = 32 * 1024
    _CRC = struct.Struct(">I")  # 4-byte
    _PLEN = struct.Struct(">H")  # 2-byte
    _SEQ = struct.Struct(">Q")  # 8-byte
    _LEN = struct.Struct(">I")  # 4-byte
    _HEADER = 7

    #: Record type lives in the physical header, not in the payload.
    _SINGLE_KIND = 0
    _BATCH_KIND_FULL = 1
    _BATCH_KIND_FIRST = 2
    _BATCH_KIND_MIDDLE = 3
    _BATCH_KIND_LAST = 4

    #: Log file path
    _path: Path

    def __init__(self, path: Path):
        self._path = path
        # create folder on disk
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if not self._path.exists():
            self._path.touch()

    def _build_physical_record(self, kind: int, payload: bytes) -> bytes:
        checksum = zlib.crc32(bytes([kind]) + payload) & 0xFFFFFFFF
        return (
            self._CRC.pack(checksum)
            + self._PLEN.pack(len(payload))
            + bytes([kind])
            + payload
        )

    def _padding_short_tail(self, f, block_off: int) -> int:
        """Zero the rest of the block when a header plus one payload byte will not fit"""
        if block_off == 0:
            return 0
        leftover = self._BLOCK_SIZE - block_off
        if leftover <= self._HEADER:
            f.write(b"\x00" * leftover)
            return 0
        return block_off

    def _append_single(self, f, body: bytes) -> None:
        block_off = self._padding_short_tail(f, f.tell() % self._BLOCK_SIZE)
        leftover = self._BLOCK_SIZE - block_off
        if len(body) > leftover - self._HEADER and block_off:
            f.write(b"\x00" * leftover)
        f.write(self._build_physical_record(self._SINGLE_KIND, body))

    def _append_batch(self, f, body: bytes) -> None:
        block_off = f.tell() % self._BLOCK_SIZE
        pos = 0
        n = len(body)
        started = False
        while pos < n:
            block_off = self._padding_short_tail(f, block_off)
            available = self._BLOCK_SIZE - block_off - self._HEADER
            take = min(n - pos, available)
            finishes = pos + take == n
            if not started and finishes:
                kind = self._BATCH_KIND_FULL
            elif not started:
                kind = self._BATCH_KIND_FIRST
            elif finishes:
                kind = self._BATCH_KIND_LAST
            else:
                kind = self._BATCH_KIND_MIDDLE
            f.write(self._build_physical_record(kind, body[pos : pos + take]))
            pos += take
            started = True
            block_off += self._HEADER + take
            if block_off == self._BLOCK_SIZE:
                block_off = 0

    async def append(
        self,
        key: bytes | WriteBatch,
        seq_num: int,
        val: bytes | None = None,
    ) -> None:
        if isinstance(key, WriteBatch):
            body = bytes(key._buf)
            is_batch = True
        else:
            if val is None:
                val = b""
            body = (
                self._SEQ.pack(seq_num)
                + self._LEN.pack(len(key))
                + self._LEN.pack(len(val))
                + key
                + val
            )
            if self._HEADER + len(body) > self._BLOCK_SIZE:
                raise ValueError("wal single record exceeds block")
            is_batch = False

        # TODO: keep one file open and append each record to it
        with self._path.open("ab") as f:
            if is_batch:
                self._append_batch(f, body)
            else:
                self._append_single(f, body)
            f.flush()

    def _physical_records(self, data: bytes) -> Iterator[tuple[int, bytes]]:
        offset = 0
        n = len(data)
        while offset < n:
            leftover = self._BLOCK_SIZE - (offset % self._BLOCK_SIZE)
            # A short tail stops after the last good record, only one record is missed, acceptable
            if leftover < self._HEADER:
                if offset + leftover < n:
                    offset += leftover
                    continue
                return
            if n - offset < self._HEADER:
                return
            if data[offset : offset + self._HEADER] == b"\x00" * self._HEADER:
                offset += leftover
                continue

            (checksum,) = self._CRC.unpack_from(data, offset)
            (payload_len,) = self._PLEN.unpack_from(data, offset + 4)
            frame_end = offset + self._HEADER + payload_len
            # Part of this frame is past the end of the file, so this record is missed
            if frame_end > n:
                return
            if self._HEADER + payload_len > leftover:
                raise ValueError("wal checksum mismatch")

            kind = data[offset + 6]
            payload = data[offset + self._HEADER : frame_end]
            offset = frame_end
            # A checksum miss with bytes after it is corruption
            if (zlib.crc32(bytes([kind]) + payload) & 0xFFFFFFFF) != checksum:
                # This frame is the end of the file, only one record is missed, acceptable
                if offset == n:
                    return
                raise ValueError("wal checksum mismatch")
            yield kind, payload

    def _single_ops(self, body: bytes) -> Iterator[tuple[bytes, int, bytes]]:
        (seq_num,) = self._SEQ.unpack_from(body, 0)
        inner = 8
        (key_len,) = self._LEN.unpack_from(body, inner)
        inner += 4
        (val_len,) = self._LEN.unpack_from(body, inner)
        inner += 4
        key = bytes(body[inner : inner + key_len])
        inner += key_len
        val = bytes(body[inner : inner + val_len])
        yield key, seq_num, val

    def _batch_ops(self, batch_body: bytes) -> Iterator[tuple[bytes, int, bytes]]:
        (seq_num,) = WriteBatch._SEQ.unpack_from(batch_body, 0)
        batch = WriteBatch()
        batch._buf = bytearray(batch_body)
        (batch.count,) = WriteBatch._COUNT.unpack_from(batch_body, 8)
        for key, value in batch:
            stored = b"" if value is TOMBSTONE else value
            yield key, seq_num, stored
            seq_num += 1

    def records(self) -> Iterator[tuple[bytes, int, bytes]]:
        """Yield each operation with its own sequence.

        A batch record stores the first operation's sequence.
        The next operation in that batch is that sequence plus one, and so on.
        """
        pending: bytearray | None = None
        for kind, payload in self._physical_records(self._path.read_bytes()):
            if kind == self._SINGLE_KIND or kind == self._BATCH_KIND_FULL:
                if pending is not None:
                    raise ValueError("wal record kind")
                if kind == self._SINGLE_KIND:
                    yield from self._single_ops(payload)
                else:
                    yield from self._batch_ops(payload)
                continue
            if kind == self._BATCH_KIND_FIRST:
                if pending is not None:
                    raise ValueError("wal record kind")
                pending = bytearray(payload)
                continue
            if kind == self._BATCH_KIND_MIDDLE or kind == self._BATCH_KIND_LAST:
                if pending is None:
                    raise ValueError("wal record kind")
                pending += payload
                if kind == self._BATCH_KIND_LAST:
                    yield from self._batch_ops(bytes(pending))
                    pending = None
                continue
            raise ValueError("wal record kind")

    # TODO: replay these sequences into the store after a crash
    async def recover(self) -> AsyncIterator[tuple[bytes, int, bytes]]:
        for record in self.records():
            yield record

    async def clear(self):
        # TODO: rotate to wal-(n+1) instead of truncating a single file
        self._path.write_bytes(b"")
