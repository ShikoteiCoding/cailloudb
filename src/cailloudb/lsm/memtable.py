from typing import Iterator

from constants import MEMTABLE_MAX_BYTES_SIZE
from custom_types import MemTableEntry
from lsm.skiplist import SkipList


class MemTable(Iterator):
    """
    In-memory structure to keep key-value pairs (sorted by key).

    A simple wrapper around `lsm.skiplist.SkipList`
    """

    #: Skiplist tree underlying the memtable
    skiplist: SkipList

    #: Skiplist configuration for max size
    max_bytes_size: int

    def __init__(self, max_bytes_size: int = MEMTABLE_MAX_BYTES_SIZE):
        self.skiplist = SkipList()
        self.max_bytes_size = max_bytes_size

    def insert(self, key: bytes, seq_num: int, value: bytes) -> None:
        """
        Append-only a new (key, value) pair.
        """
        self.skiplist.insert(key, seq_num, value)

    def get(self, key: bytes, seq_num: int) -> MemTableEntry | None:
        """
        Get a value, tombstone or None from key at or before `seq_num`.

        Behavior:
            Returns None if key is not found.
            Returns the deletion marker as a valid value.
        """
        return self.skiplist.get(key, seq_num)

    def scan(
        self, start_key: bytes | None, end_key: bytes | None, seq_num: int
    ) -> Iterator[MemTableEntry]:
        """
        Scan values, tombstone or None from key at or before `seq_num`.

        Behavior:
            start_key is inclusive, end_key is exclusive.
            Yield MemTableEntry(key, seq_num, value) for the latest valid version.
            Doesn't yield None (it is not aware of out-of-range keys)
            Ordering guarantee as the SkipList property
        """
        for entry in self.skiplist.scan(start_key, end_key, seq_num):
            yield entry

    def __iter__(self) -> Iterator[tuple[bytes, bytes]]:
        """
        Sequentially yields all entries stored in the SkipList.

        When this method is called, it is usually when the MemTable is marked as immutable.
        This happens either when the MemTable is full or when a flush is triggered.
        For this reason, no write/read conflicts are to be expected / handled here.

        Behavior:
            Ordering guarantee as the SkipList property
        """
        # for entry in self.skiplist:
        #     yield entry
        return self.skiplist.__iter__()

    def __next__(self):
        return self.skiplist.__next__()

    @property
    def bytes_size(self) -> int:
        return self.skiplist.bytes_size

    def is_full(self) -> bool:
        return self.bytes_size >= self.max_bytes_size

    def __len__(self):
        return len(self.skiplist)
