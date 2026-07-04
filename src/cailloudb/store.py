from typing import TYPE_CHECKING, AsyncIterator

from abc import ABC, abstractmethod

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
        self, start_seq: int, end_seq: int
    ) -> AsyncIterator[tuple[bytes, bytes | None]]: ...

    @abstractmethod
    async def latest_sequence_number(self) -> int: ...


class InMemoryStore(BaseStore):
    #: Internal hashmap for storing KV store
    __d: dict[bytes, bytes]
    #: Operation log indexed by sequence number
    # TODO trim old log
    __log: list[tuple[SeqNum, bytes, bytes | None]]

    def __init__(self):
        super().__init__()

        self.__d = {}
        self.__log = []
        self._seq = 0

    async def get(self, key: bytes) -> bytes:
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        return self.__d[key]

    async def put(self, key: bytes, val: bytes):
        self._seq += 1
        self.__d[key] = val
        self.__log.append((self._seq, key, val))

    async def delete(self, key: bytes):
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        self._seq += 1
        del self.__d[key]
        self.__log.append((self._seq, key, None))

    async def exists(self, key: bytes) -> bool:
        return key in self.__d

    async def write(self, batch: WriteBatch):
        for key, val in batch:
            if val:
                await self.put(key, val)
            else:
                await self.delete(key)

    async def scan(
        self, start_seq: int, end_seq: int
    ) -> AsyncIterator[tuple[bytes, bytes | None]]:
        for seq, key, val in self.__log:
            if start_seq <= seq <= end_seq:
                yield key, val

    async def latest_sequence_number(self) -> int:
        return self._seq


class ObjectStore:
    @classmethod
    def resolve(cls, addr: str) -> BaseStore:
        if addr == ":memory:":
            return InMemoryStore()

        raise ValueError("Address format {} failed to resolve".format(addr))
