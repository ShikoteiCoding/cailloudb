import heapq
import queue
import threading
import time
from pathlib import Path
from typing import Callable, Iterator

from settings import (
    Settings,
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

    #: Flush queue and task for SC on background flush
    _flush_queue: queue.Queue[MemTable]
    _flush_thread: threading.Thread | None
    _flush_enabled: bool

    #: Compaction
    _compaction_enabled: bool
    _compaction_thread: threading.Thread | None
    _compaction_interval_s: int

    #: File ID lock
    _file_id_lock: threading.Lock

    #: Lock for memory buffers
    _mem_lock: threading.Lock

    #: Lock for sstables
    _sst_lock: threading.Lock

    #: Runtime settings
    _settings: Settings

    def __init__(
        self,
        memtable_size: int | None = None,
        sstable_size: int | None = None,
        max_levels: int | None = None,
        wal_path: Path | None = None,
        flush_enabled: bool = True,
        compaction_enabled: bool = True,
        compaction_interval_s: int = 10,
        settings: Settings | None = None,
    ):
        self._settings = settings or Settings()
        memtable_size = (
            self._settings.memtable_max_bytes_size
            if memtable_size is None
            else memtable_size
        )
        sstable_size = (
            self._settings.sstable_max_file_size
            if sstable_size is None
            else sstable_size
        )
        max_levels = (
            self._settings.sstable_max_levels if max_levels is None else max_levels
        )

        self.memtable_size = memtable_size
        self.sstable_size = sstable_size
        self.max_levels = max_levels

        self._memtable = MemTable(max_bytes_size=memtable_size)
        self._immutable_memtables = []
        self._sstables_map = {}
        self._levels = [[] for _ in range(max_levels)]
        self._next_file_id = 1
        self._wal = (
            Wal(wal_path, wal_block_size=self._settings.wal_block_size)
            if wal_path is not None
            else None
        )
        self._compactor = Compactor()
        self._file_picker = FilePicker(
            self._levels,
            self.max_levels,
            sstable_l0_compaction_trigger=self._settings.sstable_l0_compaction_trigger,
            sstable_l1_max_bytes=self._settings.sstable_l1_max_bytes,
            sstable_level_multiplier=self._settings.sstable_level_multiplier,
        )

        self._flush_queue = queue.Queue()
        self._flush_thread = None
        self._flush_enabled = flush_enabled
        self._compaction_thread = None
        self._compaction_enabled = compaction_enabled
        self._compaction_interval_s = compaction_interval_s

        self._file_id_lock = threading.Lock()
        self._mem_lock = threading.Lock()
        self._sst_lock = threading.Lock()

        # XXX: Not pythonic, this should be moved to __aenter__
        # So that proper lifecycle can be handled async
        # (especially init / close)
        self._start_flush_worker()
        self._start_compaction_worker()

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
            self._flush_queue.put(self._memtable)
            self._memtable = MemTable(max_bytes_size=self.memtable_size)
            if self._wal is not None:
                self._wal.roll()

    def file_id_generator(self) -> int:
        """
        Thread-safe file ID Generator.

        XXX: Should be moved to FileManager / VersionManager when introduced.
        """
        with self._file_id_lock:
            file_id = self._next_file_id
            self._next_file_id += 1
            return file_id

    def _start_flush_worker(self) -> None:
        """
        Start a flush task in a worker if not already running.
        """
        if self._flush_thread is None:
            self._flush_thread = self._make_background_thread(self._flush_worker)
            self._flush_thread.start()

    def _make_background_thread(self, target: Callable) -> threading.Thread:
        return threading.Thread(target=target, daemon=True)

    def _flush_worker(self) -> None:
        if self._flush_queue is None:
            return

        while True:
            try:
                memtable = self._flush_queue.get()

                # XXX: Handle retries and failures
                generated_ssstables = write_iterator_to_sstables(
                    iterator=memtable.__iter__(),
                    file_id_generator=self.file_id_generator,
                    max_file_size=self.sstable_size,
                )

                with self._sst_lock:
                    for sstable, file_metadata in generated_ssstables:
                        self._sstables_map[file_metadata["file_id"]] = sstable
                        self._levels[0].append(file_metadata)
                        with self._mem_lock:
                            self._immutable_memtables.remove(memtable)

                self._flush_queue.task_done()
            except Exception:
                # XXX: Improve error handling
                pass

    def _pick_and_compact(
        self,
    ) -> tuple[
        int,
        int,
        list[FileMetaData],
        list[FileMetaData],
        list[tuple[SSTable, FileMetaData]],
    ]:
        """
        Synchronous in-process compaction task of SSTables from L0 to Lmax.

        NOTE: This is for now a full compaction (not partial) because
        it acts on the entire level, not specific files.

        Properties:
            - Key ordering guarantee inside the SSTable
            - No Key range overlap accross SSTables
        """
        with self._sst_lock:
            source_level = self._file_picker.pick_compaction_level()
            if source_level == -1:
                return -1, -1, [], [], []

            target_level = source_level + 1
            source_metas, target_metas = self._file_picker.pick_compaction_files(
                source_level, target_level
            )
            input_metas = source_metas + target_metas
            if not input_metas:
                return -1, -1, [], [], []
            input_sstables = [
                self._sstables_map[meta["file_id"]] for meta in input_metas
            ]

        return (
            source_level,
            target_level,
            source_metas,
            target_metas,
            # This can safely be outside the lock as it is purely additive
            self._compactor.compact(
                sstables_to_merge=input_sstables,
                file_id_generator=self.file_id_generator,  # XXX: resolve concurrency with flush
                level_sstable_size=self.sstable_size
                * target_level,  # XXX: naive heuristic
            ),
        )

    def _start_compaction_worker(self) -> None:
        """
        Start the compactor in a dedicated OS thread if enabled.
        """
        if not self._compaction_enabled:
            return

        if self._compaction_thread is None:
            self._compaction_thread = self._make_background_thread(
                self._compaction_worker
            )
            self._compaction_thread.start()

    def _commit_compaction_result(
        self,
        result: tuple[
            int,
            int,
            list[FileMetaData],
            list[FileMetaData],
            list[tuple[SSTable, FileMetaData]],
        ],
    ) -> None:
        source_level, target_level, source_metas, target_metas, generated_files = result
        if not generated_files:
            return

        generated_metas = []

        with self._sst_lock:
            for sstable, meta in generated_files:
                self._sstables_map[sstable.file_id] = sstable
                generated_metas.append(meta)

            self._file_picker.apply_compaction_result(
                source_level,
                target_level,
                source_metas,
                target_metas,
                generated_metas,
            )

    def _compaction_worker(self) -> None:
        """
        Threaf-safe compaction worker loop.

        On schedule:
            - Pick levels & files
            - Compact
            - Commit
        """
        while True:
            time.sleep(self._compaction_interval_s)
            self._compaction_tick()

    def _compaction_tick(self) -> None:
        # XXX: Apply retry and error handling
        self._commit_compaction_result(self._pick_and_compact())
