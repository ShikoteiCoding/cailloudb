import io
from pathlib import Path
from typing import Iterator

from constants import (
    KEY_LEN_STRUCT,
    KEY_METADATA_STRUCT,
    LEN_KEY_LEN,
    LEN_KEY_METADATA,
    LEN_VAL_LEN,
    TOMBSTONE,
    VAL_LEN_STRUCT,
)
from custom_types import MemTableEntry, SSTableEntry
from lsm.memtable import MemTable


def encode_memtable_entry(entry: MemTableEntry) -> tuple[bytearray, int]:
    """
    Encode a memtable entry according to encoding described in
        `cailloudb.lsm.sstable.SSTableWriter`.
    """
    key, seq_num, value = entry["key"], entry["seq_num"], entry["value"]

    is_tombstone = value == TOMBSTONE
    value_type = 0x0 if is_tombstone else 0x1
    val_len = 0 if is_tombstone else len(value)

    # InternalKey = Key + Metadata
    internal_key_len = len(key) + LEN_KEY_METADATA

    # Invert sequence number bits (56 bits) (same logic as SkipList)
    inverted_seq = (~seq_num) & 0x00FFFFFFFFFFFFFF
    key_metadata = (inverted_seq << 8) | value_type

    # Allocate fixed-size buffer
    total_size = (
        LEN_KEY_LEN
        + internal_key_len
        + (LEN_VAL_LEN + val_len if not is_tombstone else 0)
    )
    buf = bytearray(total_size)

    # Pack length of InternalKey (key + key metadata)
    KEY_LEN_STRUCT.pack_into(buf, 0, internal_key_len)
    offset = LEN_KEY_LEN

    # Pack key bytes
    buf[offset : offset + len(key)] = key
    offset += len(key)

    # Pack Key Metadata
    KEY_METADATA_STRUCT.pack_into(buf, offset, key_metadata)
    offset += LEN_KEY_METADATA

    # Pack Value Length + Value bytes (if not a tombstone)
    if not is_tombstone:
        VAL_LEN_STRUCT.pack_into(buf, offset, val_len)
        offset += LEN_VAL_LEN
        buf[offset : offset + val_len] = value

    return buf, total_size


class SSTableWriter:
    """
    Encodes and writes entries from cailloudb.lsm.memtable.MemTable to disk.

    Binary Layout for Entry:
        [4 bytes internal key len][internal key bytes][4 bytes val len][val bytes]
        └────────────────────────────────────────────┘└──────────────────────────┘
             InternalKey Block                           Value Block (Put only)

    InternalKey Structure (internal key len = len(key) + 8 bytes):
        [N bytes key][8 bytes metadata]

    Key Metadata Layout:
        ┌──────────────────────────────────────────────┬────────────────────────┐
        │ Bits 63-8: Bit-Inverted SeqNum (~seq_num)    │ Bits 7-0: ValueType    │
        │ (56 bits, yields DESC sorting via byte comp) │ (0x0 = Delete, 0x1=Put)│
        └──────────────────────────────────────────────┴────────────────────────┘

    Ordering Properties:
        By bit-inverting `seq_num` and encoding the key metadata, standard
        lexicographical byte comparisons natively sort keys by:
            1. Key ASC
            2. Sequence Number DESC (newest versions come first)
    """

    def __init__(self, in_memory: bool, dir: Path | None = None):
        self.in_memory = in_memory
        self.dir = dir

    def write(self, memtable: MemTable) -> SSTable:
        """
        Write each MemTableEntry of the MemTable to destination.
        """
        # Keep track of offset of each records for fast lookup
        offsets = []
        offset = 0

        path = Path()
        file = io.BytesIO()

        for entry in memtable:
            offsets.append(offset)

            buf, total_size = encode_memtable_entry(entry)
            file.write(buf)

            offset += total_size

        file.seek(0)
        if self.in_memory:
            return InMemorySSTable(file=file, path=path, offsets=offsets)
        return SSTable(file=file, path=path, offsets=offsets)


class SSTable:
    #: Reference to File Buffer object
    file: io.BytesIO

    #: Path to the file
    path: Path

    #: List of buffer offset pointers to first char of each records
    offsets: list[int]

    def __init__(self, file: io.BytesIO, path: Path, offsets: list[int]):
        self.file = file
        self.path = path
        self.offsets = offsets

        # TODO: Used as skip filter
        # self.low_key: bytes = low_key
        # self.high_key: bytes = high_key

    def __iter__(self):
        """
        Read sequentially the whole physical SSTable.

        Decode SSTable entries from disk according to encoding from
            `cailloudb.lsm.sstable.SSTableWriter`.
        """
        for offset in self.offsets:
            self.file.seek(offset)

            # Read internal key length
            internal_key_len_bytes = self.file.read(LEN_KEY_LEN)
            internal_key_len = KEY_LEN_STRUCT.unpack(internal_key_len_bytes)[0]

            # Read full internal key
            internal_key = self.file.read(internal_key_len)

            # Split into key and metadata
            key = internal_key[:-LEN_KEY_METADATA]
            key_metadata_bytes = internal_key[-LEN_KEY_METADATA:]

            # Unpack Key Metadata
            key_metadata = KEY_METADATA_STRUCT.unpack(key_metadata_bytes)[0]
            inverted_seq = key_metadata >> 8
            seq_num = (~inverted_seq) & 0x00FFFFFFFFFFFFFF
            value_type = key_metadata & 0xFF

            # Handle value / tombstone based on value_type (0x0 = Delete, 0x1 = Put)
            if value_type == 0x0:
                yield SSTableEntry(key=key, seq_num=seq_num, value=TOMBSTONE)
            else:
                val_len_bytes = self.file.read(LEN_VAL_LEN)
                val_len = VAL_LEN_STRUCT.unpack(val_len_bytes)[0]
                val = self.file.read(val_len)
                yield SSTableEntry(key=key, seq_num=seq_num, value=val)

    def get(self, key: bytes, seq_num: int) -> SSTableEntry | None:
        """
        Get the latest version of `key` at or before `seq_num`.

        Behavior:
            Returns None if key is not found.
            Returns the deletion marker as a valid value.
        """
        # Build a lookup internal key from provided maximum `seq_num``
        inverted_target_seq = (~seq_num) & 0x00FFFFFFFFFFFFFF
        target_key_metadata = (inverted_target_seq << 8) | 0x00
        target_internal_key = key + KEY_METADATA_STRUCT.pack(target_key_metadata)

        left = 0
        right = len(self.offsets) - 1
        found_offset = -1

        # Binary search over raw InternalKey byte representations
        while left <= right:
            mid = (left + right) // 2
            offset = self.offsets[mid]

            self.file.seek(offset)
            internal_key_len = KEY_LEN_STRUCT.unpack(self.file.read(LEN_KEY_LEN))[0]
            entry_internal_key = self.file.read(internal_key_len)

            if entry_internal_key >= target_internal_key:
                found_offset = offset
                right = mid - 1  # Try to find an earlier matching entry on the left
            else:
                left = mid + 1

        if found_offset == -1:
            return None

        # Read and validate the single candidate landed on
        self.file.seek(found_offset)
        internal_key_len = KEY_LEN_STRUCT.unpack(self.file.read(LEN_KEY_LEN))[0]
        entry_internal_key = self.file.read(internal_key_len)

        # Extract table key
        table_key = entry_internal_key[:-LEN_KEY_METADATA]
        if table_key != key:
            return None  # All versions of `key` were newer than `seq_num` or `key` doesn't exist

        # Unpack key metadata
        key_metadata = KEY_METADATA_STRUCT.unpack(
            entry_internal_key[-LEN_KEY_METADATA:]
        )[0]
        inverted_seq = key_metadata >> 8
        entry_seq_num = (~inverted_seq) & 0x00FFFFFFFFFFFFFF
        value_type = key_metadata & 0xFF

        # Handle deletion marker
        if value_type == 0x0:
            return SSTableEntry(key=key, seq_num=entry_seq_num, value=TOMBSTONE)

        # Read value
        val_len = VAL_LEN_STRUCT.unpack(self.file.read(LEN_VAL_LEN))[0]
        return SSTableEntry(
            key=key, seq_num=entry_seq_num, value=self.file.read(val_len)
        )

    def scan(
        self, start_key: bytes | None, end_key: bytes | None, seq_num: int
    ) -> Iterator[SSTableEntry]:
        """
        Scan values, tombstone or None from keys at or before `seq_num`.

        Behavior:
            start_key is inclusive, end_key is exclusive.
            Yield SSTableEntry(key, seq_num, value) for all the keys.
            Doesn't yield None (it is not aware of out-of-range keys)
            Ordering guarantee as the SSTable property
            Parse raw bytes to lightweight SSTableEntry typeddict
        """
        start_idx = 0

        # Binary search over raw InternalKey byte representations
        if start_key is not None:
            target_internal_key = start_key + (b"\x00" * LEN_KEY_METADATA)

            left = 0
            right = len(self.offsets) - 1
            start_idx = len(self.offsets)

            while left <= right:
                mid = (left + right) // 2
                self.file.seek(self.offsets[mid])

                ik_len = KEY_LEN_STRUCT.unpack(self.file.read(LEN_KEY_LEN))[0]
                entry_ik = self.file.read(ik_len)

                if entry_ik >= target_internal_key:
                    start_idx = mid
                    right = mid - 1  # Keep searching left for the true start
                else:
                    left = mid + 1

        # Scan Sequentially from start_idx
        for idx in range(start_idx, len(self.offsets)):
            self.file.seek(self.offsets[idx])

            ik_len = KEY_LEN_STRUCT.unpack(self.file.read(LEN_KEY_LEN))[0]
            entry_ik = self.file.read(ik_len)

            table_key = entry_ik[:-LEN_KEY_METADATA]

            # Exit if we cross the exclusive end_key (exclusive)
            if end_key is not None and table_key >= end_key:
                break

            # Parse the key metadata (seq_num and value_type)
            key_metadata = KEY_METADATA_STRUCT.unpack(entry_ik[-LEN_KEY_METADATA:])[0]
            inverted_seq = key_metadata >> 8
            entry_seq_num = (~inverted_seq) & 0x00FFFFFFFFFFFFFF
            value_type = key_metadata & 0xFF

            # Ignore newer versions
            if entry_seq_num > seq_num:
                continue

            # Yield the entry (tombstone or value)
            if value_type == 0x0:
                yield SSTableEntry(
                    key=table_key, seq_num=entry_seq_num, value=TOMBSTONE
                )
            else:
                val_len = VAL_LEN_STRUCT.unpack(self.file.read(LEN_VAL_LEN))[0]
                val = self.file.read(val_len)
                yield SSTableEntry(key=table_key, seq_num=entry_seq_num, value=val)


class InMemorySSTable(SSTable):
    """
    In-memory SSTable for `cailloudb.store.InMemoryStore`
    """

    def __init__(self, file: io.BytesIO, path: Path, offsets: list[int]):
        super().__init__(file, path, offsets)
