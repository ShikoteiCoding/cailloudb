from typing import Iterator

import struct


class WriteBatch:
    """
    WriteBatch accumulates puts / deletes to be applied atomically.

    Keeps operations ordering.

    Payload :
      [8 bytes sequence number][4 bytes record count]
      then each operation:
        [4 bytes key length][4 bytes val length][key bytes][val bytes]

    An empty value is a delete.
    The sequence number is the first operation. A batch of two operations
    uses that sequence and the next one.
    """

    # TODO: replace 4-byte key/value lengths with a cheaper encoding.
    # TODO: single-op WAL records have no batch header; decide if that path stays.

    _SEQ = struct.Struct(">Q")
    _COUNT = struct.Struct(">I")
    _LEN = struct.Struct(">I")
    _HEADER = 12

    #: sequence (8B) + count (4B) + encoded operations
    _buf: bytearray

    _count: int

    _seq: int

    def __init__(self):
        self._buf = bytearray(self._HEADER)
        self._count = 0
        self._seq = 0

    def _sync_header(self):
        self._buf[0:8] = self._SEQ.pack(self._seq)
        self._buf[8:12] = self._COUNT.pack(self._count)

    def put(self, key: bytes, val: bytes):
        self._buf += self._LEN.pack(len(key)) + self._LEN.pack(len(val)) + key + val
        self._count += 1
        self._sync_header()

    def delete(self, key: bytes):
        self._buf += self._LEN.pack(len(key)) + self._LEN.pack(0) + key
        self._count += 1
        self._sync_header()

    def clear(self):
        self._buf = bytearray(self._HEADER)
        self._count = 0
        self._seq = 0

    def __iter__(self) -> Iterator[tuple[bytes, bytes | None]]:
        buf = self._buf
        offset = self._HEADER
        n = len(buf)
        while offset < n:
            (key_len,) = self._LEN.unpack_from(buf, offset)
            offset += 4
            (val_len,) = self._LEN.unpack_from(buf, offset)
            offset += 4
            key = bytes(buf[offset : offset + key_len])
            offset += key_len
            val = bytes(buf[offset : offset + val_len])
            offset += val_len

            if val_len == 0:
                yield key, None
            else:
                yield key, val

            self._count -= 1

    def __len__(self) -> int:
        return self._count
