import bisect
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, AsyncIterator

from index import KeyIndex

if TYPE_CHECKING:
    from write_batch import WriteBatch

TOMBSTONE = b""


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


class BaseStore(ABC):
    #: Sequence number for atomic write ops (put, del, merge)
    _seq: SeqNum

    @abstractmethod
    async def get(self, key: bytes) -> bytes: ...

    @abstractmethod
    async def get_at(self, key: bytes, max_seq: int) -> bytes: ...

    @abstractmethod
    async def put(self, key: bytes, val: bytes): ...

    @abstractmethod
    async def delete(self, key: bytes): ...

    @abstractmethod
    async def exists(self, key: bytes) -> bool: ...

    @abstractmethod
    async def exists_at(self, key: bytes, max_seq: int) -> bool: ...

    @abstractmethod
    async def write(self, batch: WriteBatch): ...

    @abstractmethod
    def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]: ...

    @abstractmethod
    def scan_at(
        self,
        max_seq: int,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]: ...

    @abstractmethod
    async def latest_sequence_number(self) -> int: ...


class InMemoryStore(BaseStore):
    #: Key → (seq, value) history; empty value is a tombstone
    __d: dict[bytes, list[tuple[int, bytes]]]

    #: Sorted key index for live range scans
    __index: KeyIndex

    def __init__(self):
        super().__init__()

        self.__d = {}
        self.__index = KeyIndex()
        self._seq = SeqNum()

    def _resolve_at(self, key: bytes, max_seq: int) -> bytes:
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        for seq, val in reversed(self.__d[key]):
            if seq <= max_seq:
                if val == TOMBSTONE:
                    break
                return val

        raise KeyError("key {} not found".format(key))

    def _exists_at(self, key: bytes, max_seq: int) -> bool:
        if key not in self.__d:
            return False

        for seq, val in reversed(self.__d[key]):
            if seq <= max_seq:
                return val != TOMBSTONE

        return False

    def _keys_at(self, max_seq: int) -> list[bytes]:
        return sorted(k for k in self.__d if self._exists_at(k, max_seq))

    async def get(self, key: bytes) -> bytes:
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        _, val = self.__d[key][-1]
        if val == TOMBSTONE:
            raise KeyError("key {} not found".format(key))

        return val

    async def get_at(self, key: bytes, max_seq: int) -> bytes:
        return self._resolve_at(key, max_seq)

    async def put(self, key: bytes, val: bytes):
        self._seq.increment()
        if key not in self.__d:
            self.__d[key] = []
            self.__index.insert(key)
        self.__d[key].append((int(self._seq), val))

    async def delete(self, key: bytes):
        await self.get(key)

        self._seq.increment()
        self.__d[key].append((int(self._seq), TOMBSTONE))
        self.__index.remove(key)

    async def write(self, batch: WriteBatch):
        for key, val in batch:
            if val:
                await self.put(key, val)
            else:
                await self.delete(key)

    async def exists(self, key: bytes) -> bool:
        if key not in self.__d:
            return False

        return self.__d[key][-1][1] != TOMBSTONE

    async def exists_at(self, key: bytes, max_seq: int) -> bool:
        return self._exists_at(key, max_seq)

    async def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        for key in self.__index.range(start, end):
            yield key, self.__d[key][-1][1]

    async def scan_at(
        self,
        max_seq: int,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        keys = self._keys_at(max_seq)
        lo = bisect.bisect_left(keys, start) if start is not None else 0
        hi = bisect.bisect_left(keys, end) if end is not None else len(keys)
        for key in keys[lo:hi]:
            yield key, self._resolve_at(key, max_seq)

    async def latest_sequence_number(self) -> int:
        return int(self._seq)


class ObjectStore:
    @classmethod
    def resolve(cls, addr: str) -> BaseStore:
        if addr == ":memory:":
            return InMemoryStore()

        raise ValueError("Address format {} failed to resolve".format(addr))
