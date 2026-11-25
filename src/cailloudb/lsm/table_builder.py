import io
import os
from contextlib import closing
from pathlib import Path
from typing import Callable, Iterator

from lsm.block import BlockBuilder
from lsm.bloom_filter import BloomFilterBuilder
from lsm.custom_types import FileMetadata
from lsm.sstable import DurableSSTable, InMemorySSTable, SSTable
from settings import (
    SSTABLE_MAGIC_NUMBER,
    STRUCT_INTERNAL_KEY_LEN,
    STRUCT_INTERNAL_KEY_METADATA,
    STRUCT_SSTABLE_FOOTER,
    STRUCT_SSTABLE_INDEX_OFFSET_LEN,
)


def write_iterator_to_sstables(
    iterator: Iterator[tuple[bytes, bytes]],
    file_id_generator: Callable[[], int],
    max_file_size: int,
    target_block_size: int,
    durable_sstable: bool,
    sstable_dir: Path | None = None,
) -> list[tuple[SSTable, FileMetadata]]:
    """
    Consumes a sorted containor iterator and chunks it into multiple SSTables using TableBuilder.
    """
    results = []
    builder = None

    try:
        for internal_key, value in iterator:
            if builder is None:
                builder = TableBuilder(
                    file_id=file_id_generator(),
                    sstable_block_default_size=target_block_size,
                    durable_sstable=durable_sstable,
                    sstable_dir=sstable_dir,
                )

            builder.add(internal_key, value)

            # Split if full
            if builder.estimated_size() >= max_file_size:
                results.append(builder.finish())
                builder = None

        if builder is not None:
            results.append(builder.finish())
    finally:
        if builder is not None:
            builder.close()

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

    #: File ID reference (unique)
    file_id: int

    #: Durable SSTable flag
    durable_sstable: bool

    #: Path to actual storage
    path: Path

    #: SSTable default block size
    sstable_block_default_size: int

    _bloom_builder: BloomFilterBuilder

    def __init__(
        self,
        file_id: int,
        sstable_block_default_size: int,
        durable_sstable: bool,
        sstable_dir: Path | None = None,
    ):
        self.file_id = file_id
        self.durable_sstable = durable_sstable
        self.dir = sstable_dir or Path()
        self.block_size = sstable_block_default_size

        self.block_builder = BlockBuilder(self.block_size)
        self._bloom_builder = BloomFilterBuilder()
        self.index_entries = []
        self.current_offset = 0

        self.smallest_key: bytes = b""
        self.largest_key: bytes = b""

        if self.durable_sstable:
            self.file_name = f"{self.file_id:06d}.sst"
            self.writer = (self.dir / self.file_name).open(mode="xb")
        else:
            self.writer = io.BufferedWriter(io.BytesIO())

    def close(self) -> None:
        # After detach, the in-memory SSTable owns the stream
        if self.writer.raw is not None:
            self.writer.close()

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
        self.block_builder.add(internal_key, value)
        table_key = internal_key[: -STRUCT_INTERNAL_KEY_METADATA.size]
        last_table_key = self.largest_key[: -STRUCT_INTERNAL_KEY_METADATA.size]
        if not self.largest_key or table_key != last_table_key:
            # Include tombstones so lookups can still find deletions
            self._bloom_builder.add_key(table_key)

        # The latest added key is always the largest
        self.largest_key = internal_key

    def _flush_block(self):
        block_data, last_internal_key = self.block_builder.finalize()
        self.writer.write(block_data)
        block_len = len(block_data)
        self.index_entries.append((last_internal_key, self.current_offset, block_len))
        self.current_offset += block_len

    def estimated_size(self) -> int:
        """
        Used by the caller to decide when to split files.
        """
        return (
            self.current_offset
            + self.block_builder.estimated_size()
            + self._bloom_builder.estimated_size()
        )

    def _finish_file(self) -> int:
        if not self.block_builder.is_empty():
            self._flush_block()

        # Write index block
        index_offset = self.writer.tell()
        for last_key, block_offset, block_len in self.index_entries:
            self.writer.write(STRUCT_INTERNAL_KEY_LEN.pack(len(last_key)))
            self.writer.write(last_key)
            self.writer.write(
                STRUCT_SSTABLE_INDEX_OFFSET_LEN.pack(block_offset, block_len)
            )
        index_size = self.writer.tell() - index_offset

        # Write the Bloom filter
        self.writer.write(self._bloom_builder.finalize())

        # Write footer
        self.writer.write(
            STRUCT_SSTABLE_FOOTER.pack(index_offset, index_size, SSTABLE_MAGIC_NUMBER)
        )
        return self.writer.tell()

    def finish(self) -> tuple[SSTable, FileMetadata]:
        if self.durable_sstable:
            with closing(self):
                file_size = self._finish_file()
                self.writer.flush()
                os.fsync(self.writer.fileno())
            directory_fd = os.open(self.dir, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            sstable = DurableSSTable(
                file_id=self.file_id,
                dir=self.dir,
                file_name=self.file_name,
            )
        else:
            file_size = self._finish_file()
            raw = self.writer.detach()
            raw.seek(0)
            sstable = InMemorySSTable(
                file_id=self.file_id,
                file=io.BufferedReader(raw),
            )

        return sstable, FileMetadata(
            file_id=self.file_id,
            smallest_key=self.smallest_key,
            largest_key=self.largest_key,
            file_size=file_size,
        )
