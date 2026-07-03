from typing import Iterator

import struct

PUT_BYTE = 0
DEL_BYTE = 1


class WriteBatch:
    """
    WriteBatch accumulates puts / deletes to be applied atomically.

    Keeps operations ordering.

    Encoding (per record):
      [1 byte op_type]
      [4 bytes key length][key bytes]
      [4 bytes val length][val bytes]
    """

    _count: int

    _LEN = struct.Struct(">I")  # 4-byte unsigned

    def __init__(self):
        self._buf = bytearray()
        self._count = 0

    def put(self, key: bytes, val: bytes):
        encoded = (
            bytes([PUT_BYTE])
            + self._LEN.pack(len(key))
            + key
            + self._LEN.pack(len(val))
            + val
        )
        self._buf += encoded
        self._count += 1

    def delete(self, key: bytes):
        encoded = bytes([DEL_BYTE]) + self._LEN.pack(len(key)) + key
        self._buf += encoded
        self._count += 1

    def clear(self):
        self._buf.clear()
        self._count = 0

    def __iter__(self) -> Iterator[tuple[bytes, bytes | None]]:
        buf = self._buf
        offset = 0
        n = len(buf)
        while offset < n:
            op = buf[offset]
            offset += 1
            (key_len,) = self._LEN.unpack_from(buf, offset)
            offset += 4
            key = bytes(buf[offset : offset + key_len])
            offset += key_len

            if op == 0:
                (val_len,) = self._LEN.unpack_from(buf, offset)
                offset += 4
                val = bytes(buf[offset : offset + val_len])
                offset += val_len

                yield key, val
            else:
                yield key, None

            self._count -= 1

    def __len__(self) -> int:
        return self._count
