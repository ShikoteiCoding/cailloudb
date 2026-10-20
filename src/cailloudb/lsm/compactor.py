import heapq
from typing import Iterator

from custom_types import FileMetaData
from lsm.sstable import SSTable
from lsm.table_builder import write_iterator_to_sstables
from lsm.utils import extract_from_internal_key


class Compactor:
    """
    Compact multiple SSTables into a new level of SSTables.
    """

    def __init__(self):
        pass

    def compact(
        self,
        sstables_to_merge: list[SSTable],
        next_file_id: int,
        level_sstable_size: int,
    ) -> list[tuple[SSTable, FileMetaData]]:
        """
        Synchronous multi-way merge compaction.
        Returns a list of lightweight metadata objects for the newly written files.
        """
        # K-way merge all
        merged_iterators = heapq.merge(*sstables_to_merge, key=lambda x: x[0])

        return write_iterator_to_sstables(
            self._deduplicate_stream(merged_iterators),
            next_file_id,
            level_sstable_size,
            in_memory=True,
        )

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

            # Drop old versions of the key
            # XXX: Implement a last active snapshot guardrail
            # to avoid deleting historical values still needed
            # by active readers
            if key == last_seen_key:
                continue

            last_seen_key = key

            # TODO: drop tombstone (hard delete) on last level
            yield (internal_key, value)
