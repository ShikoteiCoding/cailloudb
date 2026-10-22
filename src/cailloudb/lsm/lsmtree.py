import heapq
from pathlib import Path
from typing import Iterator

from constants import (
    MEMTABLE_MAX_BYTES_SIZE,
    SSTABLE_MAX_FILE_SIZE,
    SSTABLE_MAX_LEVELS,
    TOMBSTONE,
)
from custom_types import MemTableEntry, SSTableEntry
from lsm.compactor import Compactor
from lsm.filepicker import FilePicker
from lsm.memtable import MemTable
from lsm.sstable import SSTable
from lsm.table_builder import FileMetaData, write_iterator_to_sstables
from lsm.utils import extract_from_internal_key
from wal import Wal
from write_batch import WriteBatch


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
    _memtable: MemTable

    #: Immutable memtables waiting to be flushed
    _immutable_memtables: list[MemTable]

    #: SSTables map from "file_id" to SSTable pointer
    _sstables_map: dict[int, SSTable]

    #: List of levels of SSTable metadata
    #: l0 -> overlapping ranges, update history, ordered by seq_num
    #: l{1...N} -> non overlapping ranges, no history, sorted by key
    _levels: list[list[FileMetaData]]

    #: Next file id for .sst
    _next_file_id: int

    #: Write-ahead log writer
    _wal: Wal | None

    #: Compactor instance
    _compactor: Compactor

    #: File picker instance
    _file_picker: FilePicker

    def __init__(
        self,
        memtable_size: int = MEMTABLE_MAX_BYTES_SIZE,
        sstable_size: int = SSTABLE_MAX_FILE_SIZE,
        max_levels: int = SSTABLE_MAX_LEVELS,
        wal_path: Path | None = None,
    ):
        self.memtable_size = memtable_size
        self.sstable_size = sstable_size
        self.max_levels = max_levels

        self._memtable = MemTable(max_bytes_size=memtable_size)
        self._immutable_memtables = []
        self._sstables_map = {}
        self._levels = [[] for _ in range(max_levels)]
        self._next_file_id = 1
        self._wal = Wal(wal_path) if wal_path is not None else None
        self._compactor = Compactor()
        self._file_picker = FilePicker(self._levels, self.max_levels)

    async def put(self, key: bytes, seq_num: int, value: bytes) -> None:
        """
        Put a key-value pair into the currently active memtable.
        """
        if self._wal is not None:
            self._wal.append(key, seq_num, value)
        self._insert(key, seq_num, value)

    async def delete(self, key: bytes, seq_num: int) -> None:
        """
        Syntactic sugar to put a tombstone marker.
        """
        if self._wal is not None:
            self._wal.append(key, seq_num, b"")
        self._insert(key, seq_num, TOMBSTONE)

    async def write(self, batch: WriteBatch, seq_num: int) -> int:
        """
        Apply each operation in the batch, starting at `seq_num`.

        Returns how many operations were applied.
        """
        batch.sync_header(seq_num)
        if self._wal is not None:
            self._wal.append(batch, seq_num)
        applied = 0
        for key, value in batch:
            self._insert(key, seq_num, value)
            seq_num += 1
            applied += 1
        self._maybe_rotate_memtable()
        return applied

    def get(self, key: bytes, seq_num: int) -> bytes | None:
        """
        Point-in-time reads.

        LSMTree is responsible for the tombstone logic.
        """
        # TODO: implement bloom filter
        # Assumptions
        # - The write path is blocking during put with a synchronous flush-on-full action,
        #   there is never any immutable memtables to check

        # Check active memtable first
        entry = self._memtable.get(key, seq_num)

        # Tombstone conversion happens here
        if entry:
            return None if entry["value"] == TOMBSTONE else entry["value"]

        # TODO: solve concurrency issue when memtables are queued for flushing
        for imm in reversed(self._immutable_memtables):
            entry = imm.get(key, seq_num)
            if entry:
                return None if entry["value"] == TOMBSTONE else entry["value"]

        # Check SSTables in reverse order
        for level_idx, level_files in enumerate(self._levels):
            if not level_files:
                continue

            if level_idx == 0:
                # Level 0 can overlap, all files must be checked
                for meta in reversed(level_files):
                    # XXX: cpu-costly, should be optimized
                    if (
                        extract_from_internal_key(meta["smallest_key"])[0]
                        <= key
                        <= extract_from_internal_key(meta["largest_key"])[0]
                    ):
                        sstable = self._sstables_map[meta["file_id"]]
                        entry = sstable.get(key, seq_num)
                        if entry:
                            return (
                                None if entry["value"] == TOMBSTONE else entry["value"]
                            )
            else:
                # Levels > 0 are non-overlapping and sorted by key range.
                for meta in level_files:
                    smallest_key = extract_from_internal_key(meta["smallest_key"])[0]
                    largest_key = extract_from_internal_key(meta["largest_key"])[0]

                    # Exit early because of no overlap property
                    if key < smallest_key:
                        break

                    if key > largest_key:
                        continue

                    sstable = self._sstables_map[meta["file_id"]]
                    entry = sstable.get(key, seq_num)
                    if entry:
                        return None if entry["value"] == TOMBSTONE else entry["value"]
                    break
        return

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
        iterators.append(self._memtable.scan(start_key, end_key, seq_num))

        # TODO: solve concurrency issue when memtables are queued for flushing
        for imm in self._immutable_memtables:
            iterators.append(imm.scan(start_key, end_key, seq_num))

        for level_idx, level_files in enumerate(self._levels):
            for meta in level_files:
                # XXX: cpu-costly, should be optimized
                file_smallest_key = extract_from_internal_key(meta["smallest_key"])[0]
                file_largest_key = extract_from_internal_key(meta["largest_key"])[0]

                if start_key is not None and file_largest_key < start_key:
                    continue

                if end_key is not None and file_smallest_key >= end_key:
                    # Continue on level 0 as key overlap will exist
                    if level_idx == 0:
                        continue
                    # Exit loop as level n+1 are non overlapping and sorted
                    else:
                        break

                sst = self._sstables_map[meta["file_id"]]
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

    def _insert(self, key: bytes, seq_num: int, value: bytes) -> None:
        """
        Apply one operation to the active memtable.
        """
        self._memtable.insert(key, seq_num, value)
        self._maybe_rotate_memtable()

    def _maybe_rotate_memtable(self) -> None:
        """
        Flush full memtable(s) to SSTable, create a new active memtable and open the next WAL

        The new log is born before flush so later writes cannot land in sealed file
        """
        if self._memtable.is_full():
            self._immutable_memtables.append(self._memtable)
            self._memtable = MemTable(max_bytes_size=self.memtable_size)
            if self._wal is not None:
                self._wal.clear()

            # XXX: remove blocking the hotpath with a sync flush
            self._sync_flush()

    def _sync_flush(self) -> None:
        """
        Synchronous in-process task to write immutable memtables to disk.

        Flush proces is uniquely responsible for appending to the l0 level.
        """
        if not self._immutable_memtables:
            return

        while self._immutable_memtables:
            # XXX: Implement a retry and raise on max retry
            memtable = self._immutable_memtables.pop(0)
            generated_files = write_iterator_to_sstables(
                iterator=memtable.__iter__(),
                start_file_id=self._next_file_id,
                max_file_size=self.sstable_size,
            )

            for sstable, meta in generated_files:
                self._sstables_map[sstable.file_id] = sstable
                self._levels[0].append(meta)

            self._next_file_id += len(generated_files)

    def _sync_compact(self) -> None:
        """
        Compaction of SSTables from L0 to Lmax.

        NOTE: This is for now a full compaction (not partial) because
        it acts on the entire level, not specific files.

        Properties:
            - Key ordering guarantee inside the SSTable
            - No Key range overlap accross SSTables
        """
        source_level = self._file_picker.pick_compaction_level()
        if source_level == -1:
            return

        target_level = source_level + 1
        source_metas, target_metas = self._file_picker.pick_compaction_files(
            source_level, target_level
        )
        input_metas = source_metas + target_metas
        if not input_metas:
            return

        input_sstables = [self._sstables_map[meta["file_id"]] for meta in input_metas]

        generated_files = self._compactor.compact(
            input_sstables,
            self._next_file_id,  # XXX: resolve concurrency with flush
            self.sstable_size * target_level,  # XXX: naive heuristic
        )

        for meta in input_metas:
            del self._sstables_map[meta["file_id"]]

        generated_metas = []

        # XXX: Implement a retry and raise on max retry
        for sstable, meta in generated_files:
            self._sstables_map[sstable.file_id] = sstable
            generated_metas.append(meta)

        # XXX: Lock the levels / map during this operation
        self._file_picker.apply_compaction_result(
            source_level,
            target_level,
            source_metas,
            target_metas,
            generated_metas,
        )

        self._next_file_id += len(generated_files)
