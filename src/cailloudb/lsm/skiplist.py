import random
from typing import Iterator

from constants import (
    LEN_METADATA,
    MAX_SEQ_NUM,
    TOMBSTONE,
)
from custom_types import MemTableEntry
from lsm.utils import build_internal_key


class _SkipNode:
    """
    Immutable storage unit for SkipList values.
    """

    __slots__ = ("internal_key", "value", "forward")

    def __init__(self, internal_key: bytes, value: bytes, level: int):
        self.internal_key = internal_key
        self.value = value
        self.forward: list[_SkipNode] = [None] * (level + 1)  # type: ignore

    @property
    def key(self) -> bytes:
        """
        Extracts the original User Key.
        """
        return self.internal_key[:-LEN_METADATA]

    @property
    def seq_num(self) -> int:
        """
        Extracts the 56-bit Sequence Number.
        """
        metadata_int = int.from_bytes(
            self.internal_key[-LEN_METADATA:], byteorder="big"
        )
        inverted_seq = metadata_int >> LEN_METADATA
        return MAX_SEQ_NUM - inverted_seq

    @property
    def value_type(self) -> int:
        """
        Extracts the operation type (Put or Delete).
        """
        return self.internal_key[-1]


class SkipList:
    """
    SkipList implementation (used in LSM-Tree Memtable).

    Behavior:
        Agnostic of the value content (accept bytes only).
        Doesn't handle tombstone and deletion marker logic.
        Logical deletion (through tombstone)
    """

    def __init__(self, max_level: int = 16, p: float = 0.5):
        self.max_level = max_level
        self.p = p
        self.header = _SkipNode(internal_key=b"", value=b"", level=self.max_level)
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

        Encodes the key and sequence number into an internal key
        ordered by key (ascending) and sequence number (descending).
        """
        internal_key = build_internal_key(key, seq_num, (value == TOMBSTONE))

        # Array to store the nodes where we drop down a level during search
        update: list[_SkipNode] = [None] * (self.max_level + 1)  # type: ignore
        current = self.header

        # Traverse the skiplist top-down / left-to-right
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].internal_key < internal_key:
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
        new_node = _SkipNode(internal_key, value, r_level)
        for i in range(r_level + 1):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node

        # Track total bytes size
        self.bytes_size += len(internal_key) + len(value)
        self._size += 1

    def get(self, key: bytes, seq_num: int) -> MemTableEntry | None:
        """
        Retrieves the latest value of a key visible at or before `seq_num`.

        Behavior:
            Returns None for keys that are not found at provided `seq_num`.
            Returns MemTableEntry(key, seq_num, value) for the latest valid version.
        """
        current = self.header

        # Traverse the skiplist top-down / left-to-right
        for i in range(self.level, -1, -1):
            while (
                current.forward[i]
                and
                # Trick, b"foo" is always less than b"foo\xff..."
                current.forward[i].internal_key < key
            ):
                current = current.forward[i]

        # Move to the first node corresponding to `key`
        current = current.forward[0]

        # Iterate through version chain for `key`
        while current is not None and current.key == key:
            if current.seq_num <= seq_num:
                # Found the last version at or before `seq_num`
                return MemTableEntry(
                    key=key, seq_num=current.seq_num, value=current.value
                )
            # Entry is too new for this `seq_num`. continue
            current = current.forward[0]

        return None

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
        current = self.header

        # Traverse the skiplist top-down / left-to-right
        if start_key is not None:
            for i in range(self.level, -1, -1):
                while (
                    current.forward[i]
                    and
                    # Trick, b"foo" is always less than b"foo\xff..."
                    current.forward[i].internal_key < start_key
                ):
                    current = current.forward[i]

        current = current.forward[0]
        while current is not None:
            if end_key is not None and current.key >= end_key:
                break

            if current.seq_num <= seq_num:
                # Found the last version at or before `seq_num`
                yield MemTableEntry(
                    key=current.key, seq_num=current.seq_num, value=current.value
                )

            current = current.forward[0]

    def __iter__(self) -> Iterator[tuple[bytes, bytes]]:
        """
        Sequentially yields all entries stored in the SkipList.

        When this method is called, it is usually when the MemTable is marked as immutable.
        This happens either when the MemTable is full or when a flush is triggered.
        For this reason, no write/read conflicts are to be expected / handled here.

        Behavior:
            Ordering guarantee as the SkipList property
        """
        current = self.header.forward[0]

        while current is not None:
            yield (current.internal_key, current.value)
            current = current.forward[0]

    def __len__(self) -> int:
        return self._size
