import io
from pathlib import Path

from constants import (
    DELETED_STRUCT,
    KEY_LEN_STRUCT,
    LEN_DELETED,
    LEN_KEY_LEN,
    LEN_SEQUENCE_NUM,
    LEN_VAL_LEN,
    SEQ_STRUCT,
    TOMBSTONE,
    VAL_LEN_STRUCT,
)
from custom_types import MemTableEntry, SSTableEntry
from lsm.memtable import MemTable


class SSTableWriter:
    """
    Encode and write entries from cailloudb.lsm.memtable.MemTable.

    Encoding for each entry:
        [4 bytes key length][key bytes]
        [8 bytes for sequence number]
        [1 byte for deleted bool]
        [4 bytes val length][val bytes] <- Can be empty
    """

    def __init__(self, in_memory: bool, dir: Path | None = None):
        self.in_memory = in_memory
        self.dir = dir

    @classmethod
    def encode_memtable_entry(cls, entry: MemTableEntry) -> tuple[bytearray, int]:
        key, seq_num, value = entry["key"], entry["seq_num"], entry["value"]

        is_tombstone = value == TOMBSTONE
        val_len = 0 if is_tombstone else len(value)

        total_size = (
            LEN_KEY_LEN
            + len(key)
            + LEN_SEQUENCE_NUM
            + LEN_DELETED
            + (LEN_VAL_LEN if val_len > 0 else 0)
            + val_len
        )
        buf = bytearray(total_size)

        # Pack key length
        KEY_LEN_STRUCT.pack_into(buf, 0, len(key))
        offset = LEN_KEY_LEN

        # Pack key
        buf[offset : offset + len(key)] = key
        offset += len(key)

        # Pack sequence number
        SEQ_STRUCT.pack_into(buf, offset, seq_num)
        offset += LEN_SEQUENCE_NUM

        # Pack tombstone bool
        DELETED_STRUCT.pack_into(buf, offset, int(is_tombstone))
        offset += LEN_DELETED

        # Pack value (if tombstone pack nothing)
        if val_len > 0:
            VAL_LEN_STRUCT.pack_into(buf, offset, val_len)
            offset += LEN_VAL_LEN
            buf[offset : offset + val_len] = value

        return buf, total_size

    def write(self, memtable: MemTable) -> SSTable:
        """
        Write each MemTableEntry of the MemTable to destination.
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

        file.seek(0)
        if self.in_memory:
            return InMemorySSTable(file=file, path=path, offsets=offsets)
        return SSTable(file=file, path=path, offsets=offsets)


class SSTable:
    def __init__(self, file: io.BytesIO, path: str, offsets: list[int]):
        self.file = file
        self.path = path
        self.offsets: list[int] = offsets

        # TODO: Used as skip filter
        # self.low_key: bytes = low_key
        # self.high_key: bytes = high_key

    def __iter__(self):
        """
        Read sequentially the whole physical SSTable.

        Used during compaction.
        """
        # Get Key
        for offset in self.offsets:
            self.file.seek(offset)

            # Read key
            key_len_bytes = self.file.read(LEN_KEY_LEN)
            key_len = KEY_LEN_STRUCT.unpack(key_len_bytes)[0]
            key = self.file.read(key_len)

            # Read sequence number
            seq_num_bytes = self.file.read(LEN_SEQUENCE_NUM)
            seq_num = int(SEQ_STRUCT.unpack(seq_num_bytes)[0])

            # Read deleted flag
            is_deleted_bytes = self.file.read(LEN_DELETED)
            is_deleted = DELETED_STRUCT.unpack(is_deleted_bytes)[0]

            # Read value / tombstone
            if is_deleted:
                yield SSTableEntry(key=key, seq_num=seq_num, value=TOMBSTONE)

            else:
                val_len_bytes = self.file.read(LEN_VAL_LEN)
                val_len = VAL_LEN_STRUCT.unpack(val_len_bytes)[0]
                val = self.file.read(val_len)
                yield SSTableEntry(key=key, seq_num=seq_num, value=val)

    def get(self, key: bytes) -> SSTableEntry | None:
        """
        Binary search over offsets.
        """
        left = 0
        right = len(self.offsets) - 1

        while left <= right:
            mid = (left + right) // 2
            offset = self.offsets[mid]

            self.file.seek(offset)

            # Unpack key length
            key_len_bytes = self.file.read(LEN_KEY_LEN)
            key_len = KEY_LEN_STRUCT.unpack(key_len_bytes)[0]

            # Read key
            table_key = self.file.read(key_len)

            if key == table_key:
                #  Read sequence number
                seq_num_bytes = self.file.read(LEN_SEQUENCE_NUM)
                seq_num = int(SEQ_STRUCT.unpack(seq_num_bytes)[0])

                # Read deleted flag
                is_deleted_bytes = self.file.read(LEN_DELETED)
                is_deleted = DELETED_STRUCT.unpack(is_deleted_bytes)[0]
                if is_deleted:
                    return SSTableEntry(key=key, seq_num=seq_num, value=TOMBSTONE)

                # Unpack value length
                val_len_bytes = self.file.read(LEN_VAL_LEN)
                val_len = VAL_LEN_STRUCT.unpack(val_len_bytes)[0]
                val = self.file.read(val_len)
                return SSTableEntry(key=key, seq_num=seq_num, value=val)

            elif key > table_key:
                left = mid + 1
            else:
                right = mid - 1

        return None


class InMemorySSTable(SSTable):
    """
    In-memory SSTable for `cailloudb.store.InMemoryStore`
    """

    def __init__(self, file: io.BytesIO, path: str, offsets: list[int]):
        super().__init__(file, path, offsets)
