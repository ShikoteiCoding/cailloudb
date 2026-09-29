import random

from custom_types import TOMBSTONE


class _SkipNode:
    """
    Smallest storage unit for SkipList.
    """

    __slots__ = ("key", "value", "forward")

    def __init__(self, key: bytes, value: bytes, level: int):
        self.key = key
        self.value = value

        #: List of pointers to forward elements
        self.forward: list[_SkipNode] = [None] * (level + 1)  # type: ignore


class SkipList:
    """
    SkipList implementation (used in LSM-Tree Memtable).
    """

    def __init__(self, max_level: int = 16, p: float = 0.5):
        self.max_level = max_level
        self.p = p
        self.header = _SkipNode(key=b"", value=b"", level=self.max_level)
        self.level = 0
        self._size = 0

    def _random_level(self) -> int:
        """
        Randomly determines the height (level) for a new node.
        """
        lvl = 0
        while random.random() < self.p and lvl < self.max_level:
            lvl += 1
        return lvl

    def put(self, key: bytes, value: bytes) -> None:
        """
        Put an element inside the SkipList.
        """
        # Array to store the nodes where we drop down a level during search
        update: list[_SkipNode] = [None] * (self.max_level + 1)  # type: ignore
        current = self.header

        # Traversal from the top-most level down to level 0
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        # Move to the position where the key should exist at level 0
        current = current.forward[0]

        # Scenario A: Key already exists. Overwrite value (or overwrite with Tombstone)
        if current is not None and current.key == key:
            current.value = value
            return

        # Scenario B: Key does not exist. Create a new node.
        r_level = self._random_level()

        # If the generated level is higher than the current max level, initialize update array
        if r_level > self.level:
            for i in range(self.level + 1, r_level + 1):
                update[i] = self.header
            self.level = r_level

        # Instantiate the new node and splice it into the forward pointers
        new_node = _SkipNode(key, value, r_level)
        for i in range(r_level + 1):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node

        self._size += 1

    def delete(self, key: bytes) -> None:
        """
        Syntactic sugar that puts a tombstone.
        """
        self.put(key, TOMBSTONE)
        self._size -= 1

    def get(self, key: bytes) -> bytes | None:
        """
        Retrieves a value. Returns None if the key doesn't exist or is a tombstone.
        """
        current = self.header

        # Traverse downwards and forwards
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]

        current = current.forward[0]

        # Key found
        if current and current.key == key:
            return current.value  # Returns None if it's a tombstone, otherwise the data

        return None

    def __len__(self) -> int:
        return self._size
