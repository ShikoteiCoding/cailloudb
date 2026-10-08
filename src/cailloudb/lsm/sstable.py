import bisect
import io
from pathlib import Path
from typing import Iterator

from constants import (
    INTERNAL_KEY_LEN_STRUCT,
    INTERNAL_KEY_METADATA_STRUCT,
    INTERNAL_KEY_VALUE_TYPE_DELETE,
    SSTABLE_BLOCK_DEFAULT_SIZE,
    SSTABLE_FOOTER_STRUCT,
    SSTABLE_INDEX_BLOCK_OFFSET_AND_LEN_STRUCT,
    SSTABLE_MAGIC_NUMBER,
    SSTABLE_MAX_FILE_SIZE,
    TOMBSTONE,
)
from custom_types import SSTableEntry
from lsm.block import BlockBuilder, BlockReader
from lsm.memtable import MemTable
from lsm.utils import build_internal_key, extract_from_internal_key


class SSTableWriter:
    def __init__(
        self,
        in_memory: bool,
        dir: Path | None = None,
        block_size: int = SSTABLE_BLOCK_DEFAULT_SIZE,
        max_file_size: int = SSTABLE_MAX_FILE_SIZE,
    ):
        self.in_memory = in_memory
        self.dir = dir
        self.block_size = block_size
        self.max_file_size = max_file_size

        # TODO: Move to settings once available
        assert max_file_size > block_size, "File size must be greater than block size"

    def write(self, memtable: MemTable, file_id: int) -> list[SSTable]:
        """
        Write a MemTable to one or more SSTables.
        Splits files when they exceed max_file_size.

        SSTable binary file layout:
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
        generated_sstables = []
        out = io.BytesIO()
        block_builder = BlockBuilder(self.block_size)

        index_entries: list[tuple[bytes, int, int]] = []
        current_offset = 0

        def finalize_current_sstable():
            """
            Helper to write the index/footer, save the file, and reset state.
            """
            nonlocal out, index_entries, current_offset, file_id

            # Write index block
            index_offset = out.tell()
            for last_key, block_offset, block_len in index_entries:
                out.write(INTERNAL_KEY_LEN_STRUCT.pack(len(last_key)))
                out.write(last_key)
                out.write(
                    SSTABLE_INDEX_BLOCK_OFFSET_AND_LEN_STRUCT.pack(
                        block_offset, block_len
                    )
                )
            index_size = out.tell() - index_offset

            # Write footer
            footer_bytes = SSTABLE_FOOTER_STRUCT.pack(
                index_offset, index_size, SSTABLE_MAGIC_NUMBER
            )
            out.write(footer_bytes)
            out.seek(0)

            generated_sstables.append(
                SSTable(file=out, path=Path(f"{file_id:06d}.sst"))
                if not self.in_memory
                else InMemorySSTable(file=out, path=Path(f"{file_id:06d}.sst"))
            )
            file_id += 1

            out = io.BytesIO()
            index_entries = []
            current_offset = 0

        for internal_key, value in memtable:
            if block_builder.is_full():
                block_data, last_internal_key = block_builder.finalize()
                out.write(block_data)

                block_len = len(block_data)
                index_entries.append((last_internal_key, current_offset, block_len))
                current_offset += block_len

                if current_offset >= self.max_file_size:
                    finalize_current_sstable()

            block_builder.add(internal_key, value)

        # Close potentially opened last block
        if not block_builder.is_empty():
            block_data, last_internal_key = block_builder.finalize()
            out.write(block_data)

            block_len = len(block_data)
            index_entries.append((last_internal_key, current_offset, block_len))
            current_offset += block_len

        # Close file
        if current_offset > 0:
            finalize_current_sstable()

        return generated_sstables


class SSTable:
    #: Reference to file buffer object
    file: io.BytesIO

    #: Path to the file
    path: Path

    #: Sparse index - last_key for each block
    index_keys: list[bytes]

    #: Sparse index - (block_offset, block_len) for each block
    index_meta: list[tuple[int, int]]

    def __init__(self, file: io.BytesIO, path: Path):
        self.file = file
        self.path = path

        self.index_keys = []
        self.index_meta = []

        self._load_index()

    def _load_index(self):
        """
        Reads the footer and parses the index block into memory.
        """
        # Read footer
        self.file.seek(-SSTABLE_FOOTER_STRUCT.size, io.SEEK_END)
        footer_bytes = self.file.read(SSTABLE_FOOTER_STRUCT.size)
        index_offset, index_size, magic = SSTABLE_FOOTER_STRUCT.unpack(footer_bytes)

        if magic != SSTABLE_MAGIC_NUMBER:
            raise ValueError(f"Invalid magic number in SSTable: {self.path}")

        # Read index
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
            blk_offset, blk_len = SSTABLE_INDEX_BLOCK_OFFSET_AND_LEN_STRUCT.unpack_from(
                index_bytes, cursor
            )
            cursor += SSTABLE_INDEX_BLOCK_OFFSET_AND_LEN_STRUCT.size

            self.index_keys.append(last_key)
            self.index_meta.append((blk_offset, blk_len))

        self.file.seek(0)

    def _read_block_from(self, offset: int, length: int) -> bytes:
        self.file.seek(offset)
        return self.file.read(length)

    def __iter__(self) -> Iterator[tuple[bytes, bytes]]:
        """
        Iter all the data blocks stored by the SSTable.
        """
        for block_offset, block_len in self.index_meta:
            block_data = self._read_block_from(block_offset, block_len)
            block_reader = BlockReader(block_data)

            for internal_key, value in block_reader:
                yield internal_key, value

    def get(self, key: bytes, seq_num: int) -> SSTableEntry | None:
        """
        Get value or tombstone at or before `seq_num`.
        """
        # Build a target internal key from key/seq_num pair
        # Passing "is_deleted" True to respect Value Type ordering
        target_internal_key = build_internal_key(key, seq_num, True)

        # Find the block through sparse index last_internal_key per block
        block_idx = bisect.bisect_left(self.index_keys, target_internal_key)
        if block_idx >= len(self.index_keys):
            return None  # Key is larger than any key in this SSTable

        # Search inside the block which might have it
        block_offset, block_len = self.index_meta[block_idx]
        block_data = self._read_block_from(block_offset, block_len)

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
        if start_key is None:
            start_block_idx = 0
        else:
            target_internal_key = start_key + (
                b"\x00" * INTERNAL_KEY_METADATA_STRUCT.size
            )
            start_block_idx = bisect.bisect_left(self.index_keys, target_internal_key)

            if start_block_idx >= len(self.index_keys):
                return  # Key is larger than any key in this SSTable

        for block_idx in range(start_block_idx, len(self.index_keys)):
            block_offset, block_len = self.index_meta[block_idx]
            block_data = self._read_block_from(block_offset, block_len)

            block_reader = BlockReader(block_data)

            for table_internal_key, table_value in block_reader:
                table_key = table_internal_key[: -INTERNAL_KEY_METADATA_STRUCT.size]

                # Continue if start_key is greater
                if start_key is not None and table_key < start_key:
                    continue

                # Exit on exclusive end_key
                if end_key is not None and table_key >= end_key:
                    return

                table_key, table_seq_num, value_type = extract_from_internal_key(
                    table_internal_key
                )

                # Ignore new versions
                if table_seq_num > seq_num:
                    continue

                val = (
                    TOMBSTONE
                    if value_type == INTERNAL_KEY_VALUE_TYPE_DELETE
                    else table_value
                )
                yield SSTableEntry(key=table_key, seq_num=table_seq_num, value=val)


class InMemorySSTable(SSTable):
    """
    In-memory SSTable for `cailloudb.store.InMemoryStore`
    """

    def __init__(self, file: io.BytesIO, path: Path):
        super().__init__(file, path)
