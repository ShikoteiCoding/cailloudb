import bisect
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, AsyncIterator

from index import KeyIndex

if TYPE_CHECKING:
    from write_batch import WriteBatch

Version = tuple[int, bytes | None]


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

    @classmethod
    def pinned(cls, value: int) -> SeqNum:
        seq = cls()
        seq._value = value
        return seq


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
    #: Key → versioned values (seq, val|None)
    __d: dict[bytes, list[Version]]

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

        val: bytes | None = None
        found = False
        for seq, version in self.__d[key]:
            if seq > max_seq:
                break
            found = True
            val = version

        if not found or val is None:
            raise KeyError("key {} not found".format(key))

        return val

    def _exists_at(self, key: bytes, max_seq: int) -> bool:
        if key not in self.__d:
            return False

        val: bytes | None = None
        found = False
        for seq, version in self.__d[key]:
            if seq > max_seq:
                break
            found = True
            val = version

        return found and val is not None

    def _keys_at(self, max_seq: int) -> list[bytes]:
        return sorted(k for k in self.__d if self._exists_at(k, max_seq))

    async def get(self, key: bytes) -> bytes:
        return await self.get_at(key, int(self._seq))

    async def get_at(self, key: bytes, max_seq: int) -> bytes:
        return self._resolve_at(key, max_seq)

    async def put(self, key: bytes, val: bytes):
        self._seq.increment()
        if key not in self.__d:
            self.__d[key] = []
            self.__index.insert(key)
        self.__d[key].append((int(self._seq), val))

    async def delete(self, key: bytes):
        if not self._exists_at(key, int(self._seq)):
            raise KeyError("key {} not found".format(key))

        self._seq.increment()
        self.__d[key].append((int(self._seq), None))
        self.__index.remove(key)

    async def write(self, batch: WriteBatch):
        for key, val in batch:
            if val:
                await self.put(key, val)
            else:
                await self.delete(key)

    async def exists(self, key: bytes) -> bool:
        return await self.exists_at(key, int(self._seq))

    async def exists_at(self, key: bytes, max_seq: int) -> bool:
        return self._exists_at(key, max_seq)

    async def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        for key in self.__index.range(start, end):
            yield key, self._resolve_at(key, int(self._seq))

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
