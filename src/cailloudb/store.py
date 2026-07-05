from typing import TYPE_CHECKING, AsyncIterator

from abc import ABC, abstractmethod

from key_index import SortedKeyIndex

if TYPE_CHECKING:
    from write_batch import WriteBatch


type SeqNum = int


class BaseStore(ABC):
    #: Sequence number
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


class InMemoryStore(BaseStore):
    #: Key → value
    __d: dict[bytes, bytes]
    #: Sorted key index for range scans
    __index: SortedKeyIndex

    def __init__(self):
        super().__init__()

        self.__d = {}
        self.__index = SortedKeyIndex()
        self._seq = 0

    async def get(self, key: bytes) -> bytes:
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        return self.__d[key]

    async def put(self, key: bytes, val: bytes):
        self._seq += 1
        if key not in self.__d:
            self.__index.insert(key)
        self.__d[key] = val

    async def delete(self, key: bytes):
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        self._seq += 1
        del self.__d[key]
        self.__index.remove(key)

    async def exists(self, key: bytes) -> bool:
        return key in self.__d

    async def write(self, batch: WriteBatch):
        for key, val in batch:
            if val:
                await self.put(key, val)
            else:
                await self.delete(key)

    async def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        for key in self.__index.range(start, end):
            yield key, self.__d[key]

    async def latest_sequence_number(self) -> int:
        return self._seq


class ObjectStore:
    @classmethod
    def resolve(cls, addr: str) -> BaseStore:
        if addr == ":memory:":
            return InMemoryStore()

        raise ValueError("Address format {} failed to resolve".format(addr))
