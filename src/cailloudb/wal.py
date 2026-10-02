import struct
import time
import zlib
from pathlib import Path
from typing import AsyncIterator, Iterator

from write_batch import TOMBSTONE, WriteBatch


class Wal:
    """
    Record Encoding:
      [4 bytes checksum][2 bytes payload length][payload]

    Payload starts with a kind byte.
    Single kind:
      [1 byte kind][8 bytes sequence number][8 bytes timestamp][4 bytes key length][4 bytes val length][key bytes][val bytes]
    Batch kind:
      [1 byte kind][8 bytes timestamp][WriteBatch payload]

    One single record is one put or one delete. An empty value is a delete.
    A batch record is one write. Recover yields one sequence per operation,
    and each operation carries the batch timestamp.
    """

    # TODO: replace 4-byte key/value lengths with a cheaper encoding
    # TODO: extend this header (for example a log number), same append/recover/clear

    _CRC = struct.Struct(">I")  # 4-byte
    _PLEN = struct.Struct(">H")  # 2-byte
    _SEQ = struct.Struct(">Q")  # 8-byte
    _TS = struct.Struct(">Q")  # 8-byte
    _LEN = struct.Struct(">I")  # 4-byte
    _HEADER = 6

    #: Record kind: one operation, or a batch of operations.
    #: An operation is a put, a delete, or a merge.
    _SINGLE_KIND = 0
    _BATCH_KIND = 1

    #: Log file path
    _path: Path

    def __init__(self, path: Path):
        self._path = path
        # create folder on disk
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if not self._path.exists():
            self._path.touch()

    def _pack_record(self, payload: bytes) -> bytes:
        checksum = zlib.crc32(payload) & 0xFFFFFFFF
        return self._CRC.pack(checksum) + self._PLEN.pack(len(payload)) + payload

    async def append(
        self,
        key: bytes | WriteBatch,
        seq_num: int,
        val: bytes | None = None,
    ) -> int:
        timestamp = int(time.time())
        if isinstance(key, WriteBatch):
            payload = (
                bytes([self._BATCH_KIND]) + self._TS.pack(timestamp) + bytes(key._buf)
            )
        else:
            if val is None:
                val = b""
            body = (
                self._SEQ.pack(seq_num)
                + self._TS.pack(timestamp)
                + self._LEN.pack(len(key))
                + self._LEN.pack(len(val))
                + key
                + val
            )
            payload = bytes([self._SINGLE_KIND]) + body
        # TODO: keep one file open and append each record to it
        with self._path.open("ab") as f:
            f.write(self._pack_record(payload))
            f.flush()
        return timestamp

    def records(self) -> Iterator[tuple[bytes, int, bytes]]:
        """Yield each operation with its own sequence.

        A batch record stores the first operation's sequence.
        The next operation in that batch is that sequence plus one, and so on.
        """
        data = self._path.read_bytes()
        offset = 0
        n = len(data)
        while offset < n:
            # A short tail stops after the last good record, only one record is missed, acceptable
            if n - offset < self._HEADER:
                return
            (checksum,) = self._CRC.unpack_from(data, offset)
            (payload_len,) = self._PLEN.unpack_from(data, offset + 4)
            frame_end = offset + self._HEADER + payload_len
            # Part of this frame is past the end of the file, so this record is missed
            if frame_end > n:
                return
            payload = data[offset + self._HEADER : frame_end]
            offset = frame_end
            # A checksum miss with bytes after it is corruption
            if (zlib.crc32(payload) & 0xFFFFFFFF) != checksum:
                # This frame is the end of the file, only one record is missed, acceptable
                if offset == n:
                    return
                raise ValueError("wal checksum mismatch")

            kind = payload[0]
            body = payload[1:]
            if kind == self._BATCH_KIND:
                (timestamp,) = self._TS.unpack_from(body, 0)
                batch_body = body[8:]
                (seq_num,) = WriteBatch._SEQ.unpack_from(batch_body, 0)
                batch = WriteBatch()
                batch._buf = bytearray(batch_body)
                (batch.count,) = WriteBatch._COUNT.unpack_from(batch_body, 8)
                for key, value in batch:
                    stored = b"" if value is TOMBSTONE else value
                    yield key, seq_num, stored
                    seq_num += 1
                continue
            if kind != self._SINGLE_KIND:
                raise ValueError("wal record kind")

            (seq_num,) = self._SEQ.unpack_from(body, 0)
            (timestamp,) = self._TS.unpack_from(body, 8)
            inner = 16
            (key_len,) = self._LEN.unpack_from(body, inner)
            inner += 4
            (val_len,) = self._LEN.unpack_from(body, inner)
            inner += 4
            key = bytes(body[inner : inner + key_len])
            inner += key_len
            val = bytes(body[inner : inner + val_len])
            yield key, seq_num, val

    # TODO: replay these sequences into the store after a crash
    async def recover(self) -> AsyncIterator[tuple[bytes, int, bytes]]:
        for record in self.records():
            yield record

    async def clear(self):
        # TODO: rotate to wal-(n+1) instead of truncating a single file
        self._path.write_bytes(b"")
