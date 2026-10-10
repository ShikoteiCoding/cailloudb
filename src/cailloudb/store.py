from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, AsyncIterator

from constants import DEFAULT_WAL
from custom_types import SeqNum
from lsm.lsmtree import LSMTree

if TYPE_CHECKING:
    from pathlib import Path

    from write_batch import WriteBatch


class BaseStore(ABC):
    #: Sequence number for atomic write ops (put, del, merge)
    _seq: SeqNum

    @abstractmethod
    async def get(self, key: bytes) -> bytes: ...

    @abstractmethod
    async def get_at(self, key: bytes, *, seq_num: int) -> bytes: ...

    @abstractmethod
    async def put(self, key: bytes, val: bytes): ...

    @abstractmethod
    async def delete(self, key: bytes): ...

    @abstractmethod
    async def write(self, batch: WriteBatch): ...

    @abstractmethod
    def scan(
        self, start: bytes | None, end: bytes | None
    ) -> AsyncIterator[tuple[bytes, bytes]]: ...

    @abstractmethod
    def scan_at(
        self, start: bytes | None, end: bytes | None, *, seq_num: int
    ) -> AsyncIterator[tuple[bytes, bytes]]: ...

    @abstractmethod
    async def latest_sequence_number(self) -> int: ...


class InMemoryStore(BaseStore):
    #: LSMTree-based storage
    __tree: LSMTree

    def __init__(self, wal_path: Path = DEFAULT_WAL):
        super().__init__()

        self.__tree = LSMTree(wal_path=wal_path)
        self._seq = SeqNum()

    async def get(self, key: bytes) -> bytes | None:
        """
        Get a key.

        If the key is not found, return None.
        """
        return self.__tree.get(key, int(self._seq))

    async def get_at(self, key: bytes, *, seq_num: int) -> bytes | None:
        """
        Positional Get key.

        If the key is not found, return None.
        """
        return self.__tree.get(key, seq_num)

    async def put(self, key: bytes, value: bytes):
        """
        Put a key/value pair.
        """
        if not isinstance(value, bytes):
            raise ValueError("Type {} invalid for value.".format(type(value)))
        if not isinstance(key, bytes):
            raise KeyError("Type {} invalid for key.".format(type(key)))
        seq_num = int(self._seq)
        self.__tree.put(key, seq_num, value)
        self._seq.increment()

    async def delete(self, key: bytes):
        """
        Delete a key.

        If the key doesn't exist, a deletion marker is created nonetheless.
        """
        seq_num = int(self._seq)
        self.__tree.delete(key, seq_num)
        self._seq.increment()

    async def write(self, batch: WriteBatch):
        """
        Write a batch of operations in a single atomic operation.
        """
        # Assumptions:
        # - Single writer make this write essentially a blocking operation.
        # - Failure to apply this method should be retried during system recovery (crash).
        # - MVCC safeguards readers consistency during write runtime.
        seq_num = int(self._seq)
        applied = self.__tree.write(batch, seq_num)
        for _ in range(applied):
            self._seq.increment()

    async def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        """
        Scan values between inclusive start and exclusive end.
        """
        for key, value in self.__tree.scan(start, end, int(self._seq)):
            yield key, value

    async def scan_at(
        self, start: bytes | None = None, end: bytes | None = None, *, seq_num: int
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        """
        Positional scan values between inclusive start and exclusive end.
        """
        for key, value in self.__tree.scan(start, end, seq_num):
            yield key, value

    async def latest_sequence_number(self) -> int:
        return int(self._seq)


class DiskStore:
    NotImplementedError()
