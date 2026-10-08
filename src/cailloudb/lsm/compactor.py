import heapq
from typing import Iterator

from lsm.sstable import SSTable
from lsm.table_builder import FileMetaData, TableBuilder
from lsm.utils import extract_from_internal_key

# Assuming this was added from the previous step
# from lsm.metadata import FileMetaData


class SSTableCompactor:
    """
    Compact multiple SSTables into a new level of SSTables.
    """

    def __init__(self):
        pass

    def compact(
        self,
        source_files: list[FileMetaData],
        target_files: list[FileMetaData],
        sstable_writer: TableBuilder,
        next_file_id: int,
        target_level: int,
    ) -> list[FileMetaData]:
        """
        Synchronous multi-way merge compaction.
        Returns a list of lightweight metadata objects for the newly written files.
        """

        # 1. Instantiate reader objects for the metadata files
        # (Assuming your SSTable class takes a file_id or file path to read)
        sstables_to_merge = []
        for meta in source_files + target_files:
            sstables_to_merge.append(SSTable(file_id=meta.file_id))

        # 2. Merge-sort the iterators
        # SSTable `__iter__` should lazily yield (internal_key, value).
        # We merge them using the internal_key as the sort order.
        merged_stream = heapq.merge(*sstables_to_merge, key=lambda x: x[0])

        # 3. Deduplicate and clean up tombstones on the fly
        clean_stream = self._deduplicate_stream(merged_stream)

        # 4. Write the results
        # The writer inherently splits the stream into multiple files if max_file_size is reached.
        new_sstables_and_metadata = sstable_writer.write(clean_stream, next_file_id)

        # 5. Extract metadata from the heavy SSTable objects to return to LSMTree
        new_metadata = []
        for sstable, file_metadata in new_sstables_and_metadata:
            new_metadata.append(file_metadata)

        return new_metadata

    def _deduplicate_stream(
        self, merged_stream: Iterator[tuple[bytes, bytes]]
    ) -> Iterator[tuple[bytes, bytes]]:
        """
        Processes a globally sorted stream of keys.
        Because internal keys sort by (user_key ASC, seq_no DESC), the newest version
        of a user_key will ALWAYS be yielded first.
        """
        last_seen_key = None

        for internal_key, value in merged_stream:
            key, _, value_type = extract_from_internal_key(internal_key)

            # If we've already seen this user key in a previous iteration,
            # this is an older version. Safely drop it.
            if key == last_seen_key:
                continue

            last_seen_key = key

            # Note on tombstones:
            # We yield tombstones (DELETE) here so they propagate down and mask
            # older data in lower levels.
            # (An advanced optimization is to drop the tombstone ONLY if we are
            # compacting into the maximum level where we know no older data exists).
            yield (internal_key, value)
