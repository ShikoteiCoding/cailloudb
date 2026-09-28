from lsm.skiplist import SkipList
from lsm.custom_types import TOMBSTONE


class MemTable:
    """
    In-memory structure to keep key-value pairs (sorted by key).
    """

    def __init__(self, max_bytes_size: int = 32 * 1024 * 1024):
        self.skiplist = SkipList()
        self.max_bytes_size = max_bytes_size
        self.bytes_size = 0
        self._size = 0

    def put(self, key: bytes, value: bytes) -> None:
        """
        Add a new (key, value) pair.

        Already existing keys are overriden.
        """
        self.skiplist.put(key, value)

    def delete(self, key: bytes) -> None:
        """
        Delete a key by writing a tombstone as the value.

        If the key doesn't exist, a tombstone is created nonetheless.
        """
        self.skiplist.delete(key)

    def get(self, key: bytes) -> bytes | None:
        """
        Get a value or tombstone from key.
        """
        val = self.skiplist.get(key)
        if val == TOMBSTONE:
            return None
        return val

    def __len__(self):
        return len(self.skiplist)
