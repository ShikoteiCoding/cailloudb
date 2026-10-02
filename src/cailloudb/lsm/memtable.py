from constants import MEMTABLE_MAX_BYTES_SIZE
from custom_types import TOMBSTONE
from lsm.skiplist import SkipList


class MemTable:
    """
    In-memory structure to keep key-value pairs (sorted by key).
    """

    def __init__(self, max_bytes_size: int = MEMTABLE_MAX_BYTES_SIZE):
        self.skiplist = SkipList()
        self.max_bytes_size = max_bytes_size

    def put(self, key: bytes, value: bytes) -> None:
        """
        Add or update a new (key, value) pair.

        Already existing keys are overriden.
        """
        self.skiplist.put(key, value)

    def delete(self, key: bytes) -> None:
        """
        Delete a key by writing a tombstone as the value.

        If the key doesn't exist, a tombstone is created nonetheless.
        """
        self.skiplist.put(key, TOMBSTONE)

    def get(self, key: bytes) -> bytes | None:
        """
        Get a value, tombstone or None from key.

        Behavior:
            None is exclusively returned for non found keys.
            Returns the deletion marker as a valid value.
        """
        return self.skiplist.get(key)

    @property
    def bytes_size(self) -> int:
        return self.skiplist.bytes_size

    def is_full(self) -> bool:
        return self.bytes_size >= self.max_bytes_size

    def __len__(self):
        return len(self.skiplist)
