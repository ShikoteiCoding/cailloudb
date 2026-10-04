import struct
from typing import Iterator

from constants import TOMBSTONE


class WriteBatch:
    """
    WriteBatch accumulates puts / deletes to be applied atomically

    Keeps operations ordering

    Payload :
      [8 bytes sequence number][4 bytes record count]
      then each operation:
        [1 byte op type][4 bytes key length][4 bytes val length][key bytes][val bytes]

    Op type is a put, a delete, or a merge
    A delete yields TOMBSTONE
    The sequence number is the first operation
    A batch of two operations uses that sequence and the next one
    """

    # TODO: replace 4-byte key/value lengths with a cheaper encoding
    # TODO: single-op WAL records have no batch header, decide if that path stays

    _SEQ = struct.Struct(">Q")
    _COUNT = struct.Struct(">I")
    _LEN = struct.Struct(">I")
    _HEADER = 12

    _PUT_BYTE = 0
    _DEL_BYTE = 1

    #: sequence (8B) + count (4B) + encoded operations
    _buf: bytearray

    # Count of operations in the batch
    count: int

    def __init__(self):

        self._buf = bytearray(self._HEADER)
        self.count = 0

    def sync_header(self, seq_num: int):
        self._buf[0:8] = self._SEQ.pack(seq_num)
        self._buf[8:12] = self._COUNT.pack(self.count)

    def put(self, key: bytes, val: bytes):
        self._buf += (
            bytes([self._PUT_BYTE])
            + self._LEN.pack(len(key))
            + self._LEN.pack(len(val))
            + key
            + val
        )
        self.count += 1

    def delete(self, key: bytes):
        self._buf += (
            bytes([self._DEL_BYTE]) + self._LEN.pack(len(key)) + self._LEN.pack(0) + key
        )
        self.count += 1

    def clear(self):
        self._buf = bytearray(self._HEADER)
        self.count = 0

    def __iter__(self) -> Iterator[tuple[bytes, bytes]]:
        buf = self._buf
        offset = self._HEADER
        n = len(buf)
        while offset < n:
            op = buf[offset]
            offset += 1
            (key_len,) = self._LEN.unpack_from(buf, offset)
            offset += 4
            (val_len,) = self._LEN.unpack_from(buf, offset)
            offset += 4
            key = bytes(buf[offset : offset + key_len])
            offset += key_len
            val = bytes(buf[offset : offset + val_len])
            offset += val_len

            if op == self._DEL_BYTE:
                yield key, TOMBSTONE
            else:
                yield key, val

            self.count -= 1

    def __len__(self) -> int:
        return self.count
