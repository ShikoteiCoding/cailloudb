from typing import Iterator

from constants import MEMTABLE_MAX_BYTES_SIZE, TOMBSTONE
from custom_types import MemTableEntry
from lsm.skiplist import SkipList


class MemTable:
    """
    In-memory structure to keep key-value pairs (sorted by key).

    A simple wrapper around `lsm.skiplist.SkipList`
    """

    def __init__(self, max_bytes_size: int = MEMTABLE_MAX_BYTES_SIZE):
        self.skiplist = SkipList()
        self.max_bytes_size = max_bytes_size

    def put(self, key: bytes, seq_num: int, value: bytes) -> None:
        """
        Add or update a new (key, value) pair.

        Already existing keys are overriden.
        """
        self.skiplist.insert(key, seq_num, value)

    def delete(self, key: bytes, seq_num: int) -> None:
        """
        Delete a key by writing a tombstone as the value.

        If the key doesn't exist, a tombstone is created nonetheless.
        """
        self.skiplist.insert(key, seq_num, TOMBSTONE)

    def get(self, key: bytes) -> MemTableEntry | None:
        """
        Get a value, tombstone or None from key.

        Behavior:
            None is exclusively returned for non found keys.
            Returns the deletion marker as a valid value.
        """
        return self.skiplist.get(key)

    def __iter__(self) -> Iterator[MemTableEntry]:
        for entry in self.skiplist:
            yield entry

    @property
    def bytes_size(self) -> int:
        return self.skiplist.bytes_size

    def is_full(self) -> bool:
        return self.bytes_size >= self.max_bytes_size

    def __len__(self):
        return len(self.skiplist)
