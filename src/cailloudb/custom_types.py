from typing import TypedDict


class MemTableEntry(TypedDict):
    key: bytes
    seq_num: int
    value: bytes


class SSTableEntry(TypedDict):
    key: bytes
    seq_num: int
    value: bytes


class SeqNum:
    """Monotonically increasing sequencer generator."""

    #: Last sequence number
    _value: int

    def __init__(self):
        self._value = 0

    def increment(self):
        self._value += 1

    def __int__(self) -> int:
        return self._value


class FileMetaData(TypedDict):
    """
    Represents an SSTable file on disk.
    """

    smallest_key: bytes
    largest_key: bytes
    file_size: int
