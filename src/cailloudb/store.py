from typing import TYPE_CHECKING, AsyncIterator

from abc import ABC, abstractmethod

from index import KeyIndex

if TYPE_CHECKING:
    from write_batch import WriteBatch


class SeqNum:
    """Monotically increasing sequencer generator."""

    #: Last sequence number
    _value: int

    def __init__(self):
        self._value = 0

    def increment(self):
        self._value += 1

    def __int__(self) -> int:
        return self._value

    def pinned_copy(self) -> "SeqNum":
        copy = SeqNum()
        copy._value = self._value
        return copy

    @classmethod
    def with_value(cls, value: int) -> "SeqNum":
        seq = cls()
        seq._value = value
        return seq


LogEntry = tuple[SeqNum, bytes, bytes | None]


class BaseStore(ABC):
    #: Sequence number for atomic write ops (put, del, merge)
    _seq: SeqNum

    @abstractmethod
    async def get(self, key: bytes) -> bytes: ...

    @abstractmethod
    async def put(self, key: bytes, val: bytes): ...

    @abstractmethod
    async def delete(self, key: bytes): ...

    @abstractmethod
    async def exists(self, key: bytes) -> bool: ...

    @abstractmethod
    async def write(self, batch: WriteBatch): ...

    @abstractmethod
    def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]: ...

    @abstractmethod
    async def latest_sequence_number(self) -> int: ...

    @abstractmethod
    def write_log(self) -> list[LogEntry]: ...

    @abstractmethod
    def pinned_sequence(self) -> SeqNum: ...


class InMemoryStore(BaseStore):
    #: Key → value
    __d: dict[bytes, bytes]

    #: Sorted key index for range scans
    __index: KeyIndex

    #: Append-only write log (seq, key, val|None)
    __log: list[LogEntry]

    def __init__(self):
        super().__init__()

        self.__d = {}
        self.__index = KeyIndex()
        self.__log = []
        self._seq = SeqNum()

    async def get(self, key: bytes) -> bytes:
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        return self.__d[key]

    async def put(self, key: bytes, val: bytes):
        self._seq.increment()
        self.__log.append((self._seq.pinned_copy(), key, val))
        if key not in self.__d:
            self.__index.insert(key)
        self.__d[key] = val

    async def delete(self, key: bytes):
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        self._seq.increment()
        self.__log.append((self._seq.pinned_copy(), key, None))
        del self.__d[key]
        self.__index.remove(key)

    async def write(self, batch: WriteBatch):
        for key, val in batch:
            if val:
                await self.put(key, val)
            else:
                await self.delete(key)

    async def exists(self, key: bytes) -> bool:
        return key in self.__d

    async def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        for key in self.__index.range(start, end):
            yield key, self.__d[key]

    async def latest_sequence_number(self) -> int:
        return int(self._seq)

    def write_log(self) -> list[LogEntry]:
        return self.__log

    def pinned_sequence(self) -> SeqNum:
        return self._seq.pinned_copy()


class ObjectStore:
    @classmethod
    def resolve(cls, addr: str) -> BaseStore:
        if addr == ":memory:":
            return InMemoryStore()

        raise ValueError("Address format {} failed to resolve".format(addr))
