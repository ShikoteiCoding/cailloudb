import io
from pathlib import Path
from typing import Iterator

from constants import (
    INTERNAL_KEY_LEN_STRUCT,
    SSTABLE_BLOCK_DEFAULT_SIZE,
    SSTABLE_FOOTER_STRUCT,
    SSTABLE_INDEX_BLOCK_OFFSET_AND_LEN_STRUCT,
    SSTABLE_MAGIC_NUMBER,
)
from custom_types import FileMetaData
from lsm.block import BlockBuilder
from lsm.sstable import InMemorySSTable, SSTable


def write_iterator_to_sstables(
    iterator: Iterator[tuple[bytes, bytes]],
    start_file_id: int,
    max_file_size: int,
    in_memory: bool = True,
) -> list[tuple[SSTable, FileMetaData]]:
    """
    Consumes a sorted containor iterator and chunks it into multiple SSTables using TableBuilder.
    """
    results = []
    current_file_id = start_file_id
    builder = None

    for internal_key, value in iterator:
        if builder is None:
            builder = TableBuilder(file_id=current_file_id, in_memory=in_memory)

        builder.add(internal_key, value)

        # Split if full
        if builder.estimated_size() >= max_file_size:
            results.append(builder.finish())
            current_file_id += 1
            builder = None

    # Flush the remaining
    if builder is not None:
        results.append(builder.finish())

    return results


class TableBuilder:
    """
    Builds a single SSTable.
    Usage:
        builder = TableBuilder(...)
        for k, v in data:
            builder.add(k, v)
        sstable, meta = builder.finish()
    """

    def __init__(
        self,
        file_id: int,
        in_memory: bool = True,
        dir: Path | None = None,
        block_size: int = SSTABLE_BLOCK_DEFAULT_SIZE,
    ):
        self.file_id = file_id
        self.in_memory = in_memory
        self.dir = dir
        self.block_size = block_size

        self.out = io.BytesIO()
        self.block_builder = BlockBuilder(self.block_size)
        self.index_entries = []
        self.current_offset = 0

        self.smallest_key: bytes = b""
        self.largest_key: bytes = b""

    def add(self, internal_key: bytes, value: bytes) -> None:
        """
        Add an entry to the file.
        """
        # Assumptions:
        #   - Adds are always called on a sorted container
        if self.block_builder.is_full():
            self._flush_block()

        if self.smallest_key == b"":
            self.smallest_key = internal_key
        # The latest added key is always the largest
        self.largest_key = internal_key

        self.block_builder.add(internal_key, value)

    def _flush_block(self):
        block_data, last_internal_key = self.block_builder.finalize()
        self.out.write(block_data)
        block_len = len(block_data)
        self.index_entries.append((last_internal_key, self.current_offset, block_len))
        self.current_offset += block_len

    def estimated_size(self) -> int:
        """
        Used by the caller to decide when to split files.
        """
        return self.current_offset + self.block_builder.estimated_size()

    def finish(self) -> tuple[SSTable, FileMetaData]:
        """
        Used by the caller to decide when to close the current file.
        """
        if not self.block_builder.is_empty():
            self._flush_block()

        # Write index block
        index_offset = self.out.tell()
        for last_key, block_offset, block_len in self.index_entries:
            self.out.write(INTERNAL_KEY_LEN_STRUCT.pack(len(last_key)))
            self.out.write(last_key)
            self.out.write(
                SSTABLE_INDEX_BLOCK_OFFSET_AND_LEN_STRUCT.pack(block_offset, block_len)
            )
        index_size = self.out.tell() - index_offset

        # Write footer
        self.out.write(
            SSTABLE_FOOTER_STRUCT.pack(index_offset, index_size, SSTABLE_MAGIC_NUMBER)
        )
        file_size = self.out.tell()
        self.out.seek(0)

        meta = FileMetaData(
            smallest_key=self.smallest_key,
            largest_key=self.largest_key,
            file_size=file_size,
        )

        sstable = (
            InMemorySSTable(
                file_id=self.file_id,
                file=self.out,
                path=Path(f"{self.file_id:06d}.sst"),
            )
            if self.in_memory
            else SSTable(
                file_id=self.file_id,
                file=self.out,
                path=Path(f"{self.file_id:06d}.sst"),
            )
        )
        return sstable, meta
