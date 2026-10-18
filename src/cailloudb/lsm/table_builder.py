import io
from pathlib import Path
from typing import Iterator

from constants import (
    INTERNAL_KEY_LEN_STRUCT,
    SSTABLE_BLOCK_DEFAULT_SIZE,
    SSTABLE_FOOTER_STRUCT,
    SSTABLE_INDEX_BLOCK_OFFSET_AND_LEN_STRUCT,
    SSTABLE_MAGIC_NUMBER,
    SSTABLE_MAX_FILE_SIZE,
)
from lsm.block import BlockBuilder
from lsm.sstable import InMemorySSTable, SSTable


class FileMetaData:
    """
    Represents an SSTable file on disk.
    """

    def __init__(self, smallest_key, largest_key, file_size):
        self.smallest_key = smallest_key
        self.largest_key = largest_key
        self.file_size = file_size


class TableBuilder:
    def __init__(
        self,
        in_memory: bool = True,
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

    def write(
        self, iterator: Iterator[tuple[bytes, bytes]], file_id: int
    ) -> list[tuple[SSTable, FileMetaData]]:
        """
        Write ant iterator of (bytes, bytes) tuples as one or more SSTables.
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

        smallest_key: bytes | None = None
        largest_key: bytes | None = None

        def finalize_current_sstable():
            """
            Helper to write the index/footer, save the file, and reset state.
            """
            nonlocal out, index_entries, current_offset, file_id
            nonlocal smallest_key, largest_key

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
            file_size = out.tell()
            out.seek(0)

            meta = FileMetaData(
                smallest_key=smallest_key,
                largest_key=largest_key,
                file_size=file_size,
            )

            generated_sstables.append(
                (
                    SSTable(file_id=file_id, file=out, path=Path(f"{file_id:06d}.sst"))
                    if not self.in_memory
                    else InMemorySSTable(
                        file_id=file_id, file=out, path=Path(f"{file_id:06d}.sst")
                    ),
                    meta,
                )
            )

            file_id += 1
            out = io.BytesIO()
            index_entries = []
            current_offset = 0
            smallest_key = None
            largest_key = None

        for internal_key, value in iterator:
            if block_builder.is_full():
                block_data, last_internal_key = block_builder.finalize()
                out.write(block_data)

                block_len = len(block_data)
                index_entries.append((last_internal_key, current_offset, block_len))
                current_offset += block_len

                if current_offset >= self.max_file_size:
                    finalize_current_sstable()

            if smallest_key is None:
                smallest_key = internal_key
            largest_key = internal_key

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
