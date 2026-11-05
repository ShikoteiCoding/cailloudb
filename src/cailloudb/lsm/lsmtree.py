import heapq
import queue
import threading
import time
from pathlib import Path
from typing import Callable, Iterator

from custom_types import MemTableEntry, SSTableEntry
from lsm.compactor import Compactor
from lsm.filepicker import FilePicker
from lsm.memtable import MemTable
from lsm.sstable import SSTable
from lsm.table_builder import FileMetadata, write_iterator_to_sstables
from lsm.version import VersionEdit, VersionSet
from lsm.wal import Wal
from settings import (
    TOMBSTONE,
    Settings,
)
from write_batch import WriteBatch


class LSMTree:
    """
    LSM Tree used in cailloudb.store. It is effectively a coordination layer between
    the memtables and sstables.

    Core assumption is single writer. So it is kept free of lock logic.
    """

    #: Runtime settings
    _settings: Settings

    #: Generic configurations
    _memtable_size: int
    _sstable_size: int
    _wal_enabled: bool
    _durable_sstable: bool

    #: Active memtable instance for in-memory O(1) writes
    _memtable: MemTable

    #: Immutable memtables waiting to be flushed
    _immutable_memtables: list[MemTable]

    #: Versions coordinator
    _versions: VersionSet

    #: SSTable state is owned by VersionSet.

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

    #: Lock for memory buffers
    _mem_lock: threading.Lock

    #: Path to metadata
    _path: Path

    def __init__(self, settings: Settings, durable_sstable: bool, path: Path):
        self._settings = settings
        self._durable_sstable = durable_sstable
        self._path = path

        self._wal_enabled = self._settings.wal_enabled
        self._memtable_size = self._settings.memtable_max_bytes_size
        self._sstable_size = self._settings.sstable_max_file_size

        self._immutable_memtables = []

        self._flush_queue = queue.Queue()
        self._flush_thread = None
        self._flush_enabled = self._settings.flush_enabled
        self._compaction_thread = None
        self._compaction_enabled = self._settings.compaction_enabled
        self._compaction_interval_s = self._settings.compaction_interval_s

        self._mem_lock = threading.Lock()

        self.init()

        # XXX: Not pythonic, this should be moved to __aenter__
        # So that proper lifecycle can be handled async
        # (especially init / close)
        self._start_flush_worker()
        self._start_compaction_worker()

    def init(self):
        self._versions = VersionSet(
            self._path,
            self._settings.sstable_max_levels,
        )

        if self._wal_enabled:
            recovery_log_number = self._versions.recovery_log_number
            print(recovery_log_number)

            # NOTE: Convention is 0 means no existing log as it starts at 1.
            if recovery_log_number == -1:
                self._wal = Wal(self._path, 1)
                self._memtable = MemTable(
                    max_bytes_size=self._memtable_size, log_number=1
                )
            else:
                self._memtable = MemTable(
                    max_bytes_size=self._memtable_size, log_number=recovery_log_number
                )
                self._replay_logs_to_memtable(recovery_log_number)
                self._wal = Wal(self._path, recovery_log_number)
        else:
            self._wal = None
            self._memtable = MemTable(max_bytes_size=self._memtable_size)

        self._compactor = Compactor(
            self._settings.sstable_block_default_size,
            self._path,
            self._durable_sstable,
        )

        self._file_picker = FilePicker(
            self._settings.sstable_max_levels,
            sstable_l0_compaction_trigger=self._settings.sstable_l0_compaction_trigger,
            sstable_l1_max_bytes=self._settings.sstable_l1_max_bytes,
            sstable_level_multiplier=self._settings.sstable_level_multiplier,
        )

    def _replay_logs_to_memtable(self, recovery_log_number: int):
        wal_reader = Wal.open_for_recovery(self._path, recovery_log_number)
        for key, seq_num, value in wal_reader.records():
            self._insert(key, seq_num, value)

    async def put(self, key: bytes, seq_num: int, value: bytes) -> None:
        """
        Put a key-value pair into the currently active memtable.
        """
        if self._wal is not None and self._wal_enabled:
            self._wal.append(key, seq_num, value)
        self._insert(key, seq_num, value)

    async def delete(self, key: bytes, seq_num: int) -> None:
        """
        Syntactic sugar to put a tombstone marker.
        """
        if self._wal is not None and self._wal_enabled:
            self._wal.append(key, seq_num, b"")
        self._insert(key, seq_num, TOMBSTONE)

    async def write(self, batch: WriteBatch, seq_num: int) -> int:
        """
        Apply each operation in the batch, starting at `seq_num`.

        Returns how many operations were applied.
        """
        batch.sync_header(seq_num)
        if self._wal is not None and self._wal_enabled:
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

        return self._versions.get(key, seq_num)

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

        iterators.append(self._versions.scan(start_key, end_key, seq_num))

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
            log_number = None
            if self._wal_enabled and self._wal:
                log_number = self._wal.roll()
            self._memtable = MemTable(
                max_bytes_size=self._memtable_size, log_number=log_number
            )

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
                edit = VersionEdit()
                # XXX: Handle retries and failures
                generated_files = write_iterator_to_sstables(
                    iterator=memtable.__iter__(),
                    file_id_generator=self._versions.file_id_generator,
                    max_file_size=self._sstable_size,
                    target_block_size=self._settings.sstable_block_default_size,
                    durable_sstable=self._durable_sstable,
                    sstable_dir=self._path,
                )

                # XXX: Refactor after getting rid of InMemorySSTable
                new_sstables = []
                for sstable, file_metadata in generated_files:
                    edit.add_file(0, file_metadata)
                    new_sstables.append(sstable)

                # commit to disk then remove persisted memtable
                with self._mem_lock:
                    self._versions.log_and_apply(edit, new_sstables)
                    self._immutable_memtables.remove(memtable)

                if self._wal and memtable.log_number:
                    self._wal.delete(memtable.log_number)

                self._flush_queue.task_done()
            except Exception:
                # XXX: Improve error handling
                pass

    def _pick_and_compact(
        self,
    ) -> tuple[
        int,
        int,
        list[FileMetadata],
        list[FileMetadata],
        list[tuple[SSTable, FileMetadata]],
    ]:
        """
        Synchronous in-process compaction task of SSTables from L0 to Lmax.

        NOTE: This is for now a full compaction (not partial) because
        it acts on the entire source level, not specific files.

        Properties:
            - Key ordering guarantee inside the SSTable
            - No Key range overlap accross SSTables
        """

        version = self._versions.get_current_version()
        source_level = self._file_picker.pick_compaction_level(version)
        if source_level == -1:
            return -1, -1, [], [], []

        target_level = source_level + 1
        source_metas_to_compact, target_metas_to_compact = (
            self._file_picker.pick_compaction_files(source_level, target_level, version)
        )
        input_metas_to_compact = source_metas_to_compact + target_metas_to_compact
        if not input_metas_to_compact:
            return -1, -1, [], [], []
        input_sstables_to_compact = [
            self._versions.get_sstable_map()[meta["file_id"]]
            for meta in input_metas_to_compact
        ]

        return (
            source_level,
            target_level,
            source_metas_to_compact,
            target_metas_to_compact,
            self._compactor.compact(
                sstables_to_merge=input_sstables_to_compact,
                file_id_generator=self._versions.file_id_generator,  # XXX: resolve concurrency with flush
                level_sstable_size=self._sstable_size
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
            list[FileMetadata],
            list[FileMetadata],
            list[tuple[SSTable, FileMetadata]],
        ],
    ) -> None:
        (
            source_level,
            target_level,
            source_metas_compacted,
            target_metas_compacted,
            generated_files,
        ) = result
        if not generated_files:
            return

        edit = VersionEdit()

        sstables = []
        for sstable, file_metadata in generated_files:
            edit.add_file(target_level, file_metadata)
            sstables.append(sstable)

        for source_metadata in source_metas_compacted:
            edit.delete_file(source_level, source_metadata["file_id"])
        for target_metadata in target_metas_compacted:
            edit.delete_file(target_level, target_metadata["file_id"])

        self._versions.log_and_apply(edit, sstables)

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
            self._commit_compaction_result(self._pick_and_compact())
