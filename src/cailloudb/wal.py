import zlib
from pathlib import Path
from typing import AsyncIterator, Iterator

from settings import (
    STRUCT_WAL_CRC,
    STRUCT_WAL_LEN,
    STRUCT_WAL_PAYLOAD_LEN,
    STRUCT_WAL_SEQ,
    WAL_BATCH_KIND_FIRST,
    WAL_BATCH_KIND_FULL,
    WAL_BATCH_KIND_LAST,
    WAL_BATCH_KIND_MIDDLE,
    WAL_HEADER_SIZE,
    WAL_SINGLE_KIND,
)
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
      [8 bytes sequence number][4 bytes key length][4 bytes value length][key bytes][value bytes]
    Batch payload:
      [WriteBatch payload]

    One single record is one put or one delete. An empty value is a delete.
    A batch that does not fit is split into FIRST, MIDDLE, and LAST fragments.
    Recover joins those fragments, then yields one sequence per operation.
    """

    # TODO: replace 4-byte key/value lengths with a cheaper encoding
    # TODO: extend this header (for example a log number), same append/recover/roll

    #: Active log number
    _number: int

    #: Active log file path
    _path: Path

    #: Effective WAL block size
    _wal_block_size: int

    def __init__(
        self,
        path: Path,
        wal_block_size: int,
    ):
        self._wal_block_size = wal_block_size

        # create folder on disk
        path.mkdir(parents=True, exist_ok=True)
        self._number = self._existing_number(path) + 1
        self._path = path / f"wal_{self._number:06d}.log"
        # init or crash, a new WAL file is always created
        self._path.touch()

    def _existing_number(self, path: Path) -> int:
        highest = 0
        for entry in path.glob("wal_*.log"):
            digits = entry.stem.removeprefix("wal_")
            if len(digits) == 6 and digits.isdigit():
                highest = max(highest, int(digits))
        return highest

    def _build_physical_record(self, kind: int, payload: bytes) -> bytes:
        checksum = zlib.crc32(bytes([kind]) + payload) & 0xFFFFFFFF
        return (
            STRUCT_WAL_CRC.pack(checksum)
            + STRUCT_WAL_PAYLOAD_LEN.pack(len(payload))
            + bytes([kind])
            + payload
        )

    def _padding_short_tail(self, f, block_off: int) -> int:
        """Zero the rest of the block when a header plus one payload byte will not fit"""
        if block_off == 0:
            return 0
        leftover = self._wal_block_size - block_off
        if leftover <= WAL_HEADER_SIZE:
            f.write(b"\x00" * leftover)
            return 0
        return block_off

    def _append_single(self, f, body: bytes) -> None:
        block_off = self._padding_short_tail(f, f.tell() % self._wal_block_size)
        leftover = self._wal_block_size - block_off
        if len(body) > leftover - WAL_HEADER_SIZE and block_off:
            f.write(b"\x00" * leftover)
        f.write(self._build_physical_record(WAL_SINGLE_KIND, body))

    def _append_batch(self, f, body: bytes) -> None:
        block_off = f.tell() % self._wal_block_size
        pos = 0
        n = len(body)
        started = False
        while pos < n:
            block_off = self._padding_short_tail(f, block_off)
            available = self._wal_block_size - block_off - WAL_HEADER_SIZE
            take = min(n - pos, available)
            finishes = pos + take == n
            if not started and finishes:
                kind = WAL_BATCH_KIND_FULL
            elif not started:
                kind = WAL_BATCH_KIND_FIRST
            elif finishes:
                kind = WAL_BATCH_KIND_LAST
            else:
                kind = WAL_BATCH_KIND_MIDDLE
            f.write(self._build_physical_record(kind, body[pos : pos + take]))
            pos += take
            started = True
            block_off += WAL_HEADER_SIZE + take
            if block_off == self._wal_block_size:
                block_off = 0

    def append(
        self,
        key: bytes | WriteBatch,
        seq_num: int,
        value: bytes | None = None,
    ) -> None:
        if isinstance(key, WriteBatch):
            body = bytes(key._buf)
            is_batch = True
        else:
            if value is None:
                value = b""
            body = (
                STRUCT_WAL_SEQ.pack(seq_num)
                + STRUCT_WAL_LEN.pack(len(key))
                + STRUCT_WAL_LEN.pack(len(value))
                + key
                + value
            )
            if WAL_HEADER_SIZE + len(body) > self._wal_block_size:
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
            leftover = self._wal_block_size - (offset % self._wal_block_size)
            # A short tail stops after the last good record, only one record is missed, acceptable
            if leftover < WAL_HEADER_SIZE:
                if offset + leftover < n:
                    offset += leftover
                    continue
                return
            if n - offset < WAL_HEADER_SIZE:
                return
            if data[offset : offset + WAL_HEADER_SIZE] == b"\x00" * WAL_HEADER_SIZE:
                offset += leftover
                continue

            (checksum,) = STRUCT_WAL_CRC.unpack_from(data, offset)
            (payload_len,) = STRUCT_WAL_PAYLOAD_LEN.unpack_from(data, offset + 4)
            frame_end = offset + WAL_HEADER_SIZE + payload_len
            # Part of this frame is past the end of the file, so this record is missed
            if frame_end > n:
                return
            if WAL_HEADER_SIZE + payload_len > leftover:
                raise ValueError("wal checksum mismatch")

            kind = data[offset + 6]
            payload = data[offset + WAL_HEADER_SIZE : frame_end]
            offset = frame_end
            # A checksum miss with bytes after it is corruption
            if (zlib.crc32(bytes([kind]) + payload) & 0xFFFFFFFF) != checksum:
                # This frame is the end of the file, only one record is missed, acceptable
                if offset == n:
                    return
                raise ValueError("wal checksum mismatch")
            yield kind, payload

    # TODO : add TOMBSTONE here later
    def _single_ops(self, body: bytes) -> Iterator[tuple[bytes, int, bytes]]:
        (seq_num,) = STRUCT_WAL_SEQ.unpack_from(body, 0)
        inner = 8
        (key_len,) = STRUCT_WAL_LEN.unpack_from(body, inner)
        inner += 4
        (val_len,) = STRUCT_WAL_LEN.unpack_from(body, inner)
        inner += 4
        key = bytes(body[inner : inner + key_len])
        inner += key_len
        value = bytes(body[inner : inner + val_len])
        yield key, seq_num, value

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
            if pending is None:
                if kind == WAL_SINGLE_KIND:
                    yield from self._single_ops(payload)
                elif kind == WAL_BATCH_KIND_FULL:
                    yield from self._batch_ops(payload)
                elif kind == WAL_BATCH_KIND_FIRST:
                    pending = bytearray(payload)
                else:
                    raise ValueError(
                        "Unknown `kind` from WAL file, probably because the file is corrupted."
                    )
                continue
            if kind == WAL_BATCH_KIND_MIDDLE or kind == WAL_BATCH_KIND_LAST:
                pending += payload
                if kind == WAL_BATCH_KIND_LAST:
                    yield from self._batch_ops(bytes(pending))
                    pending = None
                continue
            raise ValueError(
                "Unknown `kind` from WAL file, probably because the file is corrupted."
            )

    # TODO: replay these sequences into the store after a crash
    async def recover(self) -> AsyncIterator[tuple[bytes, int, bytes]]:
        for record in self.records():
            yield record

    def roll(self):
        """Birth new WAL file, logs stay on disk"""
        # TODO: TTL and Size Limit, for point-in-time recovery
        # only archive for now
        self._number += 1
        self._path = self._path.parent / f"wal_{self._number:06d}.log"
        self._path.touch()
