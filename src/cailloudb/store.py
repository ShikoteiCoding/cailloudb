import bisect
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator

from index import KeyIndex
from wal import Wal
from constants import TOMBSTONE

if TYPE_CHECKING:
    from write_batch import WriteBatch

_DEFAULT_WAL = Path("cailloudb-data") / "wal"


class SeqNum:
    """Monotonically increasing sequencer generator."""

    #: Last sequence number
    _value: int

    def __init__(self, value: int = 0):
        self._value = value

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
    #: Key → event history; put has "bytes", delete does not
    __d: dict[bytes, list[dict]]

    #: Sorted key index for live range scans
    __index: KeyIndex

    #: Write-ahead log
    _wal: Wal

    def __init__(self, wal_path: Path = _DEFAULT_WAL):
        super().__init__()

        self.__d = {}
        self.__index = KeyIndex()
        self._seq = SeqNum()
        self._wal = Wal(wal_path)

    def _resolve_at(self, key: bytes, max_seq: int) -> bytes:
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        for event in reversed(self.__d[key]):
            if event["seq"] >= max_seq:
                continue
            if "bytes" in event:
                return event["bytes"]
            raise KeyError("key {} not found".format(key))

        raise KeyError("key {} not found".format(key))

    def _exists_at(self, key: bytes, max_seq: int) -> bool:
        try:
            self._resolve_at(key, max_seq)
        except KeyError:
            return False
        return True

    def _keys_at(self, max_seq: int) -> list[bytes]:
        return sorted(k for k in self.__d if self._exists_at(k, max_seq))

    async def get(self, key: bytes) -> bytes:
        return self._resolve_at(key, int(self._seq))

    async def get_at(self, key: bytes, max_seq: int) -> bytes:
        return self._resolve_at(key, max_seq)

    def _apply_put(self, key: bytes, seq_num: int, value: bytes):
        if key not in self.__d:
            self.__d[key] = []
            self.__index.insert(key)

        self.__d[key].append({"seq": seq_num, "bytes": value})
        self._seq.increment()

    def _apply_delete(self, key: bytes, seq_num: int):
        self.__d[key].append({"seq": seq_num})
        self.__index.remove(key)
        self._seq.increment()

    async def put(self, key: bytes, value: bytes):
        if not isinstance(value, bytes):
            raise ValueError("Type {} invalid for value.".format(type(value)))
        if not isinstance(key, bytes):
            raise KeyError("Type {} invalid for key.".format(type(key)))
        seq_num = int(self._seq)
        await self._wal.append(key, seq_num, value)
        self._apply_put(key, seq_num, value)

    async def delete(self, key: bytes):
        await self.get(key)
        seq_num = int(self._seq)
        await self._wal.append(key, seq_num, b"")
        self._apply_delete(key, seq_num)

    async def write(self, batch: WriteBatch):
        seq_num = int(self._seq)
        batch._sync_header(seq_num)
        await self._wal.append(batch, seq_num)
        for i, (key, value) in enumerate(batch):
            if value == TOMBSTONE:
                self._apply_delete(key, seq_num + i)
            else:
                self._apply_put(key, seq_num + i, value)

    async def exists(self, key: bytes) -> bool:
        return self._exists_at(key, int(self._seq))

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


class DiskStore:
    @classmethod
    def resolve(cls, addr: str) -> BaseStore:
        if addr == ":memory:":
            return InMemoryStore()

        raise ValueError("Address format {} failed to resolve".format(addr))
