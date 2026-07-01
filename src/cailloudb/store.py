from abc import ABC, abstractmethod


class Store(ABC):
    @abstractmethod
    async def get(self, key: bytes) -> bytes: ...

    @abstractmethod
    async def put(self, key: bytes, val: bytes): ...

    @abstractmethod
    async def delete(self, key: bytes): ...

    @abstractmethod
    async def exists(self, key: bytes) -> bool: ...


class InMemoryStore(Store):
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


class ObjectStore:
    @classmethod
    def resolve(cls, addr: str) -> Store:
        if addr == ":memory:":
            return InMemoryStore()

        raise ValueError("Address format {} failed to resolve".format(addr))
