import heapq
from typing import Iterator

from constants import (
    MEMTABLE_MAX_BYTES_SIZE,
    SSTABLE_MAX_FILE_SIZE,
    SSTABLE_MAX_LEVELS,
    TOMBSTONE,
)
from custom_types import MemTableEntry, SSTableEntry
from lsm.memtable import MemTable
from lsm.sstable import SSTable
from lsm.table_builder import FileMetaData, TableBuilder


class LSMTree:
    """
    LSM Tree used in cailloudb.store. It is effectively a coordination layer between
    the memtables and sstables.

    Core assumption is single writer. So it is kept free of lock logic.
    """

    memtable_size: int
    sstable_size: int
    max_levels: int

    #: Active memtable instance for in-memory O(1) writes
    memtable: MemTable

    #: Immutable memtables waiting to be flushed
    immutable_memtables: list[MemTable]

    #: TableBuilder
    table_builder: TableBuilder

    #: SSTables map from "file_id" to SSTable pointer
    sstables_map: dict[int, SSTable]

    #: List of levels of SSTable metadata
    #: l0 -> overlapping ranges, update history, ordered by seq_num
    #: l{1...N} -> non overlapping ranges, no history, sorted by key
    levels: list[list[FileMetaData]]

    #: Next file id for .sst
    next_file_id: int

    def __init__(
        self,
        memtable_size: int = MEMTABLE_MAX_BYTES_SIZE,
        sstable_size: int = SSTABLE_MAX_FILE_SIZE,
        max_levels: int = SSTABLE_MAX_LEVELS,
    ):
        self.memtable_size = memtable_size
        self.sstable_size = sstable_size
        self.max_levels = max_levels

        self.memtable = MemTable(max_bytes_size=memtable_size)
        self.immutable_memtables = []
        self.table_builder = TableBuilder(in_memory=True)
        self.sstables_map = {}
        self.levels = [[] for _ in range(max_levels)]
        self.next_file_id = 1

    def put(self, key: bytes, seq_num: int, value: bytes) -> None:
        """
        Put a key-value pair into the currently active memtable.
        """

        # TODO: Write to wal

        self.memtable.insert(key, seq_num, value)
        self._maybe_rotate_memtable()

    def delete(self, key: bytes, seq_num: int) -> None:
        """
        Syntactic sugar to put a tombstone marker.
        """
        self.memtable.insert(key, seq_num, TOMBSTONE)
        self._maybe_rotate_memtable()

    def get(self, key: bytes, seq_num: int) -> bytes | None:
        """
        Point-in-time reads.

        LSMTree is responsible for the tombstone logic.
        """
        # Assumptions
        # - The write path is blocking during put with a synchronous flush-on-full action,
        #   there is never any immutable memtables to check

        # Check active memtable first
        entry = self.memtable.get(key, seq_num)

        # Tombstone conversion happens here
        if entry:
            if entry["value"] == TOMBSTONE:
                return None
            return entry["value"]

        # Check SSTables in reverse order
        # TODO: solve concurrency issue when memtables are queued for flushing
        # TODO: implement bloom filter

        # XXX: improve get to use SSTableMetadata
        for sstable in reversed(self.sstables_map.values()):
            entry = sstable.get(key, seq_num)

            # Tombstone conversion happens here
            if entry:
                if entry["value"] == TOMBSTONE:
                    return None
                return entry["value"]

        return None

    def scan(
        self, start_key: bytes | None, end_key: bytes | None, seq_num: int
    ) -> Iterator[tuple[bytes, bytes]]:
        """
        Ranged point-in-time reads.

        Behavior:
            start_key is inclusive, end_key is exclusive.
        """
        # Assumptions
        # - The write path is blocking during put with a synchronous flush-on-full action,
        #   there is never any immutable memtables to check.

        # Get all iterators
        iterators = []
        iterators.append(self.memtable.scan(start_key, end_key, seq_num))

        # XXX: improve get to use SSTableMetadata
        for sst in self.sstables_map.values():
            iterators.append(sst.scan(start_key, end_key, seq_num))

        # Aggregator function for whichever iterator
        def iterator_agg(iterator: Iterator[MemTableEntry | SSTableEntry]):
            for entry in iterator:
                if end_key is not None and entry["key"] >= end_key:
                    break  # Stop streaming from this component if we pass end_key

                # Trick, reverse the seq num to ensure last wins
                yield (entry["key"], -entry["seq_num"], entry["value"])

        # K-way merge
        merged_entries = heapq.merge(*[iterator_agg(it) for it in iterators])

        last_processed_key = None

        for key, neg_seq, value in merged_entries:
            entry_seq = -neg_seq

            if entry_seq > seq_num:
                continue  # Skip newer versions

            if key == last_processed_key:
                continue  # Skip key duplicates

            # Mask internal tombstone to downstream
            if value != TOMBSTONE:
                yield key, value

            last_processed_key = key

    def _maybe_rotate_memtable(self) -> None:
        """
        Flush full memtable(s) to SSTable and create a new active memtable.
        """
        if self.memtable.is_full():
            self.immutable_memtables.append(self.memtable)
            self.memtable = MemTable(max_bytes_size=self.memtable_size)
            self._sync_flush()

    def _sync_flush(self) -> None:
        """
        Synchronous in-process task to write immutable memtables to disk.

        Flush proces is uniquely responsible for appending to the l0 level.
        """
        if not self.immutable_memtables:
            return

        while self.immutable_memtables:
            memtable = self.immutable_memtables.pop(0)
            new_sstables = self.table_builder.write(
                memtable.__iter__(), self.next_file_id
            )
            self.next_file_id += len(new_sstables)

            for sstable, file_metadata in new_sstables:
                self.sstables_map[sstable.file_id] = sstable
                self.levels[0].append(file_metadata)
