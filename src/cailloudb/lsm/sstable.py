import bisect
import io
from pathlib import Path
from typing import Iterator

from constants import (
    DEFAULT_BLOCK_SIZE,
    FOOTER_STRUCT,
    INDEX_BLOCK_OFFSET_AND_LEN_LEN,
    INDEX_BLOCK_OFFSET_AND_LEN_STRUCT,
    INTERNAL_KEY_LEN_LEN,
    INTERNAL_KEY_LEN_STRUCT,
    KEY_METADATA_STRUCT,
    LEN_KEY_METADATA,
    MAGIC_NUMBER,
    MAX_SEQ_NUM,
    SSTABLE_FOOTER_SIZE,
    SSTABLE_MAX_FILE_SIZE,
    TOMBSTONE,
    VALUE_LEN_LEN,
    VALUE_LEN_STRUCT,
)
from custom_types import MemTableEntry, SSTableEntry
from lsm.block import BlockBuilder, BlockReader
from lsm.memtable import MemTable

# def encode_memtable_entry(entry: MemTableEntry) -> tuple[bytearray, int]:
#     """
#     Encode a memtable entry according to encoding described in
#         `cailloudb.lsm.sstable.SSTableWriter`.
#     """
#     key, seq_num, value = entry["key"], entry["seq_num"], entry["value"]

#     is_tombstone = value == TOMBSTONE
#     value_type = 0x0 if is_tombstone else 0x1
#     val_len = 0 if is_tombstone else len(value)

#     # InternalKey = Key + Metadata
#     internal_key_len = len(key) + LEN_KEY_METADATA

#     # Invert sequence number bits (56 bits) (same logic as SkipList)
#     inverted_seq = (~seq_num) & 0x00FFFFFFFFFFFFFF
#     key_metadata = (inverted_seq << 8) | value_type

#     # Allocate fixed-size buffer
#     total_size = (
#         LEN_KEY_LEN
#         + internal_key_len
#         + (LEN_VAL_LEN + val_len if not is_tombstone else 0)
#     )
#     buf = bytearray(total_size)

#     # Pack length of InternalKey (key + key metadata)
#     INTERNAL_KEY_LEN_STRUCT.pack_into(buf, 0, internal_key_len)
#     offset = LEN_KEY_LEN

#     # Pack key bytes
#     buf[offset : offset + len(key)] = key
#     offset += len(key)

#     # Pack Key Metadata
#     KEY_METADATA_STRUCT.pack_into(buf, offset, key_metadata)
#     offset += LEN_KEY_METADATA

#     # Pack Value Length + Value bytes (if not a tombstone)
#     if not is_tombstone:
#         VAL_LEN_STRUCT.pack_into(buf, offset, val_len)
#         offset += LEN_VAL_LEN
#         buf[offset : offset + val_len] = value

#     return buf, total_size


# class SSTableWriter:
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

SSTable binary file layout

+------------------------------------------------------------------------+
| Data Block 0 (~4KB of length-prefixed MemTableEntries)                 |
+------------------------------------------------------------------------+
| Data Block 1 (~4KB of length-prefixed MemTableEntries)                 |
+------------------------------------------------------------------------+
| ...                                                                    |
+------------------------------------------------------------------------+
| Data Block N (Final partial data block)                                |
+------------------------------------------------------------------------+
| Index Block: Array of [4B KeyLen][LastKey][8B BlockOffset][8B BlockLen]|
+------------------------------------------------------------------------+
| Footer (20 Bytes Fixed): [8B IndexOffset][8B IndexSize][4B Magic]      |
+------------------------------------------------------------------------+
"""


class SSTableWriter:
    def __init__(
        self,
        in_memory: bool,
        dir: Path | None = None,
        block_size: int = DEFAULT_BLOCK_SIZE,
        max_file_size: int = SSTABLE_MAX_FILE_SIZE,
    ):
        self.in_memory = in_memory
        self.dir = dir
        self.block_size = block_size
        self.max_file_size = max_file_size

    def write(self, memtable: MemTable, file_id: int) -> list[SSTable]:
        """
        Write a MemTable to one or more SSTables.
        Splits files when they exceed max_file_size.
        """
        generated_sstables = []
        out = io.BytesIO()
        block_builder = BlockBuilder(self.block_size)

        index_entries: list[tuple[bytes, int, int]] = []
        current_offset = 0

        def finalize_current_sstable():
            """Helper to write the index/footer, save the file, and reset state."""
            nonlocal out, index_entries, current_offset, file_id

            # --- 2. Write Index Block ---
            index_offset = out.tell()
            for last_key, blk_offset, blk_len in index_entries:
                out.write(INTERNAL_KEY_LEN_STRUCT.pack(len(last_key)))
                out.write(last_key)
                out.write(INDEX_BLOCK_OFFSET_AND_LEN_STRUCT.pack(blk_offset, blk_len))

            index_size = out.tell() - index_offset

            # --- 3. Write Footer ---
            footer_bytes = FOOTER_STRUCT.pack(index_offset, index_size, MAGIC_NUMBER)
            out.write(footer_bytes)

            # Rewind and finalize the SSTable object
            out.seek(0)

            # Note: You can format the path here if writing to disk
            generated_sstables.append(
                SSTable(file=out, path=Path(f"{file_id:06d}.sst"))
            )
            file_id += 1

            # --- 4. Reset state for the next SSTable ---
            out = io.BytesIO()
            index_entries = []
            current_offset = 0

        # --- 1. Stream Data Blocks ---
        for internal_key, value in memtable:
            # Flush current block if threshold is met BEFORE adding new entry
            if block_builder.is_full():
                block_data, last_internal_key = block_builder.reset()
                out.write(block_data)

                block_len = len(block_data)
                index_entries.append((last_internal_key, current_offset, block_len))
                current_offset += block_len

                # CHECK FOR FILE SPLIT
                if current_offset >= self.max_file_size:
                    finalize_current_sstable()

            # Add entry to active block buffer
            block_builder.add(internal_key, value)

        # Flush final partial block if present
        if not block_builder.is_empty():
            block_data, last_internal_key = block_builder.reset()
            out.write(block_data)

            block_len = len(block_data)
            index_entries.append((last_internal_key, current_offset, block_len))
            current_offset += block_len

        # Finalize the last file (if it contains any data)
        # We check > 0 to avoid generating an empty SSTable if the previous
        # file split happened exactly on the last record of the MemTable.
        if current_offset > 0:
            finalize_current_sstable()

        return generated_sstables

    # def write(self, memtable: MemTable) -> SSTable:
    #     """
    #     Write each MemTableEntry of the MemTable to destination.
    #     """
    #     # Keep track of offset of each records for fast lookup
    #     offsets = []
    #     offset = 0

    #     path = Path()
    #     file = io.BytesIO()

    #     for entry in memtable:
    #         offsets.append(offset)

    #         buf, total_size = encode_memtable_entry(entry)
    #         file.write(buf)

    #         offset += total_size

    #     file.seek(0)
    #     if self.in_memory:
    #         return InMemorySSTable(file=file, path=path, offsets=offsets)
    #     return SSTable(file=file, path=path, offsets=offsets)


class SSTable:
    #: Reference to File Buffer object
    file: io.BytesIO

    #: Path to the file
    path: Path

    #: Sparse Index - (last_key, offset, length) for each block
    index_keys: list[bytes]

    def __init__(self, file: io.BytesIO, path: Path):
        self.file = file
        self.path = path

        self.index_keys: list[bytes] = []
        self.index_meta: list[tuple[int, int]] = []  # (offset, length)

        self._load_index()

        # reset buffer offset
        self.file.seek(0)

    def _load_index(self):
        """Reads the footer and parses the index block into memory."""
        # 1. Read Footer
        # Seek backward from the EOF by the exact size of the footer struct
        self.file.seek(-FOOTER_STRUCT.size, io.SEEK_END)
        footer_bytes = self.file.read(FOOTER_STRUCT.size)
        index_offset, index_size, magic = FOOTER_STRUCT.unpack(footer_bytes)

        if magic != MAGIC_NUMBER:
            raise ValueError(f"Invalid magic number in SSTable: {self.path}")

        # 2. Read the entire Index Block
        self.file.seek(index_offset)
        index_bytes = self.file.read(index_size)

        # 3. Parse Index Entries
        cursor = 0
        while cursor < index_size:
            # Read internal key length
            key_len = INTERNAL_KEY_LEN_STRUCT.unpack_from(index_bytes, cursor)[0]
            cursor += INTERNAL_KEY_LEN_STRUCT.size

            # Read the actual key
            last_key = index_bytes[cursor : cursor + key_len]
            cursor += key_len

            # Read the data block offset and length
            blk_offset, blk_len = INDEX_BLOCK_OFFSET_AND_LEN_STRUCT.unpack_from(
                index_bytes, cursor
            )
            cursor += INDEX_BLOCK_OFFSET_AND_LEN_STRUCT.size

            self.index_keys.append(last_key)
            self.index_meta.append((blk_offset, blk_len))

    def _read_block(self, offset: int, length: int) -> bytes:
        """Helper to fetch a raw data block."""
        self.file.seek(offset)
        return self.file.read(length)

    def __iter__(self) -> Iterator[tuple[bytes, bytes]]:
        """
        Sequential scan over all records in the SSTable.
        """
        for blk_offset, blk_len in self.index_meta:
            # Read the full block
            block_data = self._read_block(blk_offset, blk_len)

            # Delegate parsing to the BlockReader
            block_reader = BlockReader(block_data)

            # Yield entries exactly as they appear
            for internal_key, value in block_reader:
                yield internal_key, value

    def get(self, key: bytes, seq_num: int) -> SSTableEntry | None:
        """
        Point lookup using Binary Search.
        """
        if not self.index_keys:
            return None

        # 1. Build target internal key for binary searching the block index.
        # Type byte 0x00 ensures we target the lowest byte representation for this sequence number.
        inverted_seq = MAX_SEQ_NUM - seq_num
        target_metadata = (inverted_seq << KEY_METADATA_STRUCT.size) | 0x00
        target_internal_key = key + KEY_METADATA_STRUCT.pack(target_metadata)

        # 2. Find the candidate block
        block_idx = bisect.bisect_left(self.index_keys, target_internal_key)
        if block_idx >= len(self.index_keys):
            return None  # Key is larger than any key in this SSTable

        # 3. Fetch block data & search inside the block
        blk_offset, blk_len = self.index_meta[block_idx]
        block_data = self._read_block(blk_offset, blk_len)

        block_reader = BlockReader(block_data)
        table_value, table_seq_num, is_tombstone = block_reader.get(key, seq_num)

        if not is_tombstone and table_value is None:
            return

        return SSTableEntry(
            key=key,
            seq_num=table_seq_num,  # type: ignore
            value=table_value if table_value and not is_tombstone else TOMBSTONE,
        )

    def scan(
        self, start_key: bytes | None, end_key: bytes | None, seq_num: int
    ) -> Iterator[SSTableEntry]:
        """
        Scan values, tombstone or None from keys at or before `seq_num`.

        Behavior:
            start_key is inclusive, end_key is exclusive.
            Yield SSTableEntry(key, seq_num, value) for all the keys.
            Ordering guarantee as the SSTable property.
            Parse raw bytes to lightweight SSTableEntry typeddict.
            Yield all updates of a same key. Upstream handles dedup if needed.
        """
        if not self.index_keys:
            return

        if start_key is None:
            start_block_idx = 0
        else:
            target_internal_key = start_key + (b"\x00" * KEY_METADATA_STRUCT.size)
            start_block_idx = bisect.bisect_left(self.index_keys, target_internal_key)

            if start_block_idx >= len(self.index_keys):
                return

        # 2. Iterate block-by-block starting from target block
        for block_idx in range(start_block_idx, len(self.index_keys)):
            blk_offset, blk_len = self.index_meta[block_idx]
            block_data = self._read_block(blk_offset, blk_len)

            block_reader = BlockReader(block_data)

            # 3. Scan entries inside the 4 KB block
            for internal_key, raw_value in block_reader:
                table_key = internal_key[:-LEN_KEY_METADATA]

                # Filter keys prior to start_key (for early keys in the first block)
                if start_key is not None and table_key < start_key:
                    continue

                # Exit immediately if we cross the exclusive upper bound
                if end_key is not None and table_key >= end_key:
                    return

                # Unpack sequence number & type using bitwise inversion
                key_metadata = KEY_METADATA_STRUCT.unpack(
                    internal_key[-LEN_KEY_METADATA:]
                )[0]
                inverted_seq = key_metadata >> 8
                entry_seq_num = (~inverted_seq) & 0x00FFFFFFFFFFFFFF
                value_type = key_metadata & 0xFF

                # Ignore versions produced after the read snapshot
                if entry_seq_num > seq_num:
                    continue

                # Yield active value or TOMBSTONE constant
                val = TOMBSTONE if value_type == 0x0 else raw_value

                yield SSTableEntry(key=table_key, seq_num=entry_seq_num, value=val)


class InMemorySSTable(SSTable):
    """
    In-memory SSTable for `cailloudb.store.InMemoryStore`
    """

    def __init__(self, file: io.BytesIO, path: Path):
        super().__init__(file, path)
