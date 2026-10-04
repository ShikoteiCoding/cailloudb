import heapq
from typing import Iterator

from constants import TOMBSTONE
from lsm.memtable import MemTable
from lsm.sstable import SSTable, SSTableWriter


class LSMTree:
    """
    LSM Tree used in cailloudb.store. It is effectively a coordination layer between
    the memtables and sstables.

    Core assumption is single writer. So it is kept free of lock logic.
    """

    def __init__(self, memtable_size: int = 32 * 1024 * 1024):
        self.memtable_size = memtable_size

        #: Active memtable instance for in-memory O(1) writes
        self.memtable = MemTable(max_bytes_size=memtable_size)

        #: Immutable memtables waiting to be flushed
        self.immutable_memtables: list[MemTable] = []

        #: SSTable writer
        self.sstable_writer: SSTableWriter = SSTableWriter(in_memory=True)

        #: List of SSTables
        self.sstables: list[SSTable] = []

    def put(self, key: bytes, seq_num: int, value: bytes) -> None:
        """
        Put a key-value pair into the currently active memtable.
        """

        # TODO: Write to wal

        self.memtable.insert(key, seq_num, value)
        if self.memtable.is_full():
            self._rotate_memtable()

    def delete(self, key: bytes, seq_num: int) -> None:
        """
        Syntactic sugar to put a tombstone marker.
        """
        self.memtable.insert(key, seq_num, TOMBSTONE)
        if self.memtable.is_full():
            self._rotate_memtable()

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
            if entry == TOMBSTONE:
                return None
            return entry["value"]

        # Check SSTables in reverse order
        # TODO: solve concurrency issue when memtables are queued for flushing
        # TODO: implement bloom filter
        for sstable in reversed(self.sstables):
            entry = sstable.get(key, seq_num)

            # Tombstone conversion happens here
            if entry:
                if entry == TOMBSTONE:
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

        # Check SSTable in reverse orders
        # 1. Collect underlying iterators that SEEK directly to start_key
        # (You must implement .scan(start_key) on MemTable and SSTable)
        iterators = []
        iterators.append(self.memtable.scan(start_key, end_key, seq_num))

        for sst in self.sstables:
            iterators.append(sst.scan(start_key, end_key, seq_num))

        # 2. Wrapper to format entries for Python's min-heap (heapq.merge)
        def stream_wrapper(iterator):
            for entry in iterator:
                if entry.key >= end_key:
                    break  # Stop streaming from this component if we pass end_key

                # Yield tuple: (key ASC, -seq_num DESC, value)
                # By negating the seq_num, Python's native min-heap correctly
                # surfaces the NEWEST version of a key first!
                yield (entry.key, -entry.seq_num, entry.value)

        # 3. K-Way merge of all sorted streams
        merged_stream = heapq.merge(*[stream_wrapper(it) for it in iterators])

        last_processed_key = None

        # 4. The MVCC Evaluation Loop
        for key, neg_seq, value in merged_stream:
            entry_seq = -neg_seq

            # Rule A: Future Gate. If this version was written after our snapshot, skip it.
            if entry_seq > seq_num:
                continue

            # Rule B: Masking (Deduplication). If we already evaluated a version of this key,
            # it means we already saw a NEWER, valid version. Skip this older historical version.
            if key == last_processed_key:
                continue

            # We are now looking at the NEWEST valid version of this key for our snapshot.
            last_processed_key = key

            # Rule C: Tombstone shadowing. If the latest valid version is a delete marker,
            # we don't yield it, but we MUST keep last_processed_key updated so that
            # older versions in SSTables get skipped by Rule B.
            if value != TOMBSTONE:
                yield key, value

    def _rotate_memtable(self) -> None:
        """
        Flush full memtable(s) to SSTable and create a new active memtable.
        """
        self.immutable_memtables.append(self.memtable)
        self.memtable = MemTable(max_bytes_size=self.memtable_size)

        # TODO: Rotate the wal file ?

        # TODO: should it run in the background ?
        self._flush()

    def _flush(self) -> None:
        """
        Background task to write immutable memtables to disk.

        TODO: implement first compaction
        """
        if not self.immutable_memtables:
            return

        while self.immutable_memtables:
            memtable = self.immutable_memtables.pop()
            self.sstables.append(self.sstable_writer.write(memtable))
            # TODO: Delete the corresponding wal file ?
