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
        K-way merge list of sstables and write new ones.
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
        Dedup keys for low levels and keep latest seq_num.
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
