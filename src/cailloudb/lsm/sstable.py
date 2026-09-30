import io
import struct
from pathlib import Path

from constants import (
    _KEY_LEN_STRUCT,
    _LEN_KEY_LEN,
    _LEN_SEQUENCE_NUM,
    _LEN_VAL_LEN,
    _SEQ_STRUCT,
    _VAL_LEN_STRUCT,
    TOMBSTONE,
)
from custom_types import MemTableEntry, SSTableEntry
from lsm.memtable import MemTable


class SSTableWriter:
    def __init__(self, in_memory: bool, dir: Path | None):
        self.in_memory = in_memory
        self.dir = dir

    @classmethod
    def encode_memtable_entry(cls, entry: MemTableEntry) -> tuple[bytearray, int]:
        key, seq_num, value = entry["key"], entry["seq_num"], entry["value"]

        is_tombstone = value == TOMBSTONE
        val_len = 0 if is_tombstone else len(value)

        total_size = (
            _LEN_KEY_LEN + len(key) + _LEN_SEQUENCE_NUM + _LEN_VAL_LEN + len(value)
        )
        buf = bytearray(total_size)

        # Pack key
        _KEY_LEN_STRUCT.pack_into(buf, 0, len(key))
        offset = 4
        buf[offset : offset + len(key)] = key

        # Pack sequence number
        offset += len(key)
        _SEQ_STRUCT.pack_into(buf, offset, seq_num)

        # Pack value (if tombstone pack nothing)
        offset += 8
        _VAL_LEN_STRUCT.pack_into(buf, offset, len(value))
        offset += 4
        if val_len > 0:
            buf[offset : offset + val_len] = value

        return buf, total_size

    def write(self, memtable: MemTable) -> SSTable:
        """
        Write each MemTableEntries of the MemTable to destination.

        Encoding for each entry:
            [4 bytes key length][key bytes]
            [8 bytes for sequence number]
            [4 bytes val length][val bytes] <- Can be empty
        """

        # Keep track of offset of each records for fast lookup
        offsets = []
        offset = 0

        path = ""
        file = io.BytesIO()

        for entry in memtable:
            offsets.append(offset)

            buf, total_size = self.encode_memtable_entry(entry)
            file.write(buf)

            offset += total_size

        if self.in_memory:
            return InMemorySSTable(file=file, path=path, offsets=offsets)
        return SSTable(file=file, path=path, offsets=offsets)


class SSTable:
    """ """

    def __init__(self, file: io.BytesIO, path: str, offsets: list[int]):
        self.file = file
        self.path = path
        self.offsets: list[int] = offsets

        # TODO: Used for bloom filter
        # self.low_key: bytes = low_key
        # self.high_key: bytes = high_key

    def __iter__(self):
        """
        Read sequentially the whole physical SSTable.

        Used during compaction.
        """
        raise NotImplementedError()


class InMemorySSTable(SSTable):
    """
    In-memory SSTable for `cailloudb.store.InMemoryStore`
    """

    def __init__(self, file: io.BytesIO, path: str, offsets: list[int]):
        super().__init__(file, path, offsets)

    def get(self, key: bytes) -> SSTableEntry | None:
        pass
