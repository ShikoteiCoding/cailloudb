from typing import TYPE_CHECKING

from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from write_batch import WriteBatch


class BaseStore(ABC):
    @abstractmethod
    async def get(self, key: bytes) -> bytes: ...

    @abstractmethod
    async def put(self, key: bytes, val: bytes): ...

    @abstractmethod
    async def delete(self, key: bytes): ...

    @abstractmethod
    async def exists(self, key: bytes) -> bool: ...

    @abstractmethod
    async def write(self, batch: WriteBatch):
        return


class InMemoryStore(BaseStore):
    #: Internal hashmap for storing KV store
    __d: dict[bytes, bytes]

    def __init__(self):
        super().__init__()

        self.__d = {}

    async def get(self, key: bytes) -> bytes:
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        return self.__d[key]

    async def put(self, key: bytes, val: bytes):
        self.__d[key] = val

    async def delete(self, key: bytes):
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        del self.__d[key]

    async def exists(self, key: bytes) -> bool:
        return key in self.__d

    async def write(self, batch: WriteBatch):
        for key, val in batch:
            if val:
                await self.put(key, val)
            else:
                await self.delete(key)


class ObjectStore:
    @classmethod
    def resolve(cls, addr: str) -> BaseStore:
        if addr == ":memory:":
            return InMemoryStore()

        raise ValueError("Address format {} failed to resolve".format(addr))
