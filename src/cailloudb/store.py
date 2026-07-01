from typing import Any

from abc import ABC, abstractmethod


class Store(ABC):
    @abstractmethod
    def get(self, key) -> Any: ...

    @abstractmethod
    def put(self, key, val): ...

    @abstractmethod
    def delete(self, key): ...

    @abstractmethod
    def exists(self, key) -> bool: ...


class InMemoryStore(Store):
    __d: dict

    def __init__(self):
        super().__init__()

        self.__d = {}

    def get(self, key):
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        return self.__d[key]

    def put(self, key, value):
        self.__d[key] = value

    def delete(self, key):
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        del self.__d[key]

    def exists(self, key) -> bool:
        return key in self.__d
