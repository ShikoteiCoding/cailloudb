from lsm.memtable import MemTable
from lsm.sstable import SSTable, SSTableWriter


class LSMTree:
    """
    LSM Tree used in cailloudb.store

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

        self.memtable.put(key, seq_num, value)
        if self.memtable.is_full():
            self._rotate_memtable()

    def delete(self, key: bytes, seq_num: int) -> None:
        """
        Syntactic sugar to put a tombstone marker.
        """
        self.memtable.delete(key, seq_num)
        if self.memtable.is_full():
            self._rotate_memtable()

    def get(self, key: bytes) -> bytes | None:
        # Check active memtable first
        entry = self.memtable.get(key)

        # Tombstone is valid
        if entry:
            return entry["value"]

        # Check SSTables in reverse order
        # TODO: solve concurrency issue when skiplist are queued for flushing
        # TODO: implement bloom filter
        for sstable in reversed(self.sstables):
            entry = sstable.get(key)

            if entry:
                return entry["value"]

        return None

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
