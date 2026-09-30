import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from custom_types import SeqNum

#: Length of sequence number contributing to key-version total length
_LEN_SEQUENCE_NUM = 8


class _SkipNode:
    """
    Immutable storage unit for SkipList values.
    """

    __slots__ = ("composite_key", "value", "forward")

    def __init__(self, composite_key: bytes, value: bytes, level: int):
        self.composite_key = composite_key
        self.value = value

        #: List of pointers to forward elements
        self.forward: list[_SkipNode] = [None] * (level + 1)  # type: ignore

    # Keep as properties to keep memory footprint lower
    # the trade-off is runtime cpu
    @property
    def key(self) -> bytes:
        return self.composite_key[:-_LEN_SEQUENCE_NUM]

    @property
    def seq_num(self) -> int:
        inverted_seq = int.from_bytes(
            self.composite_key[-_LEN_SEQUENCE_NUM:], byteorder="big"
        )
        return int(0xFFFFFFFFFFFFFFFF - inverted_seq)


class SkipList:
    """
    SkipList implementation (used in LSM-Tree Memtable).

    Behavior:
        Agnostic of the value content (accept bytes only).
        Doesn't handle tombstone and deletion marker logic.
        Logical deletion (through tombstone)
    """

    #: Length of sequence number contributing to key-version total length
    _LEN_SEQUENCE_NUM = 8

    def __init__(self, max_level: int = 16, p: float = 0.5):
        self.max_level = max_level
        self.p = p
        self.header = _SkipNode(composite_key=b"", value=b"", level=self.max_level)
        self.level = 0
        self._size = 0
        self.bytes_size = 0

    def _random_level(self) -> int:
        """
        Randomly determines the height (level) for a new node.
        """
        lvl = 0
        while random.random() < self.p and lvl < self.max_level:
            lvl += 1
        return lvl

    def insert(self, key: bytes, seq_num: int, value: bytes) -> None:
        """
        Insert a key/value pair along with its sequence number.

        Encodes the key and sequence number into a composite internal key
        ordered by key (ascending) and sequence number (descending).
        """
        # Reverse the alphabetical order of the version so it is descending (latest first)
        descending_seq = (0xFFFFFFFFFFFFFFFF - seq_num).to_bytes(8, byteorder="big")
        composite_key = key + descending_seq

        # Array to store the nodes where we drop down a level during search
        update: list[_SkipNode] = [None] * (self.max_level + 1)  # type: ignore
        current = self.header

        # Traverse the skiplist top-down / left-to-right
        for i in range(self.level, -1, -1):
            while (
                current.forward[i] and current.forward[i].composite_key < composite_key
            ):
                current = current.forward[i]
            update[i] = current

        # Generate structural height for the new node
        r_level = self._random_level()

        # If the generated level is higher than the current max level, initialize update array
        if r_level > self.level:
            for i in range(self.level + 1, r_level + 1):
                update[i] = self.header
            self.level = r_level

        # Instantiate the new node and splice it into the forward pointers
        new_node = _SkipNode(composite_key, value, r_level)
        for i in range(r_level + 1):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node

        # Track total bytes (including the 8-byte sequence tag) and size
        self.bytes_size += len(composite_key) + len(value)
        self._size += 1

    def get(self, key: bytes) -> tuple[int, bytes] | None:
        """
        Retrieves the latest value of a key along with its sequence number.

        Behavior:
            None is exclusively returned for keys that are not found.
            Returns (seq_num, value) if found, where value might be a tombstone.
        """
        current = self.header

        # Traverse the skiplist top-down / left-to-right
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].composite_key < key:
                current = current.forward[i]
        current = current.forward[0]

        if (
            current is not None
            and current.key == key
            and len(current.composite_key) == len(key) + self._LEN_SEQUENCE_NUM
        ):
            return current.seq_num, current.value

        return None

    def __len__(self) -> int:
        return self._size
