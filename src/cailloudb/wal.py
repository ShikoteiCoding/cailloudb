import struct
import zlib
from pathlib import Path
from typing import AsyncIterator, Iterator


class Wal:
    """
    Disk write-ahead log.

    Record:
      [4 bytes checksum][2 bytes payload length][payload]

    Payload:
      [1 byte kind][8 bytes sequence number][4 bytes key length][4 bytes val length][key bytes][val bytes]

    One record is one put or one delete. An empty value is a delete.
    """

    # TODO: replace 4-byte key/value lengths with a cheaper encoding.
    # TODO: extend this header (for example a log number), same append/recover/clear.
    # TODO: log a WriteBatch as one record.

    _CRC = struct.Struct(">I")  # 4-byte big-endian checksum.
    _PLEN = struct.Struct(">H")  # 2-byte big-endian payload length.
    _SEQ = struct.Struct(">Q")  # 8-byte big-endian sequence number.
    _LEN = struct.Struct(">I")  # 4-byte big-endian key or value length.
    _HEADER = 6

    #: First payload byte.
    _SINGLE = 0

    #: Log file path
    _path: Path

    def __init__(self, path: Path):
        self._path = path
        # create folder on disk if doesn't exist
        # TODO: improve this part in future
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if not self._path.exists():
            self._path.touch()

    def _pack_record(self, payload: bytes) -> bytes:
        checksum = zlib.crc32(payload) & 0xFFFFFFFF
        return self._CRC.pack(checksum) + self._PLEN.pack(len(payload)) + payload

    async def append(self, key: bytes, val: bytes | None = None, seq: int = 0):
        if val is None:
            val = b""
        body = (
            self._SEQ.pack(seq)
            + self._LEN.pack(len(key))
            + self._LEN.pack(len(val))
            + key
            + val
        )
        payload = bytes([self._SINGLE]) + body
        # TODO: keep one file open and append each record to it.
        with self._path.open("ab") as f:
            f.write(self._pack_record(payload))
            f.flush()

    def records(self) -> Iterator[tuple[int, bytes, bytes]]:
        """Yield each operation with its own sequence."""
        data = self._path.read_bytes()
        offset = 0
        n = len(data)
        while offset < n:
            # A short tail stops after the last good record.
            if n - offset < self._HEADER:
                return
            (checksum,) = self._CRC.unpack_from(data, offset)
            (payload_len,) = self._PLEN.unpack_from(data, offset + 4)
            frame_end = offset + self._HEADER + payload_len
            # Part of this frame is past the end of the file, so this record is missed.
            if frame_end > n:
                return
            payload = data[offset + self._HEADER : frame_end]
            offset = frame_end
            # A checksum miss with bytes after it is corruption.
            if (zlib.crc32(payload) & 0xFFFFFFFF) != checksum:
                # This frame is the end of the file, so only this one record is missed.
                if offset == n:
                    return
                raise ValueError("wal checksum mismatch")

            kind = payload[0]
            body = payload[1:]
            if kind != self._SINGLE:
                raise ValueError("wal record kind")

            (seq,) = self._SEQ.unpack_from(body, 0)
            inner = 8
            (key_len,) = self._LEN.unpack_from(body, inner)
            inner += 4
            (val_len,) = self._LEN.unpack_from(body, inner)
            inner += 4
            key = bytes(body[inner : inner + key_len])
            inner += key_len
            val = bytes(body[inner : inner + val_len])
            yield seq, key, val

    # TODO: replay these sequences into the store after a crash.
    async def recover(self) -> AsyncIterator[tuple[int, bytes, bytes]]:
        for record in self.records():
            yield record

    async def clear(self):
        # TODO: rotate to wal-(n+1) instead of truncating a single file.
        self._path.write_bytes(b"")
