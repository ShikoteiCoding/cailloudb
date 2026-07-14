from typing import TYPE_CHECKING, AsyncIterator, NamedTuple

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


LogEntry = tuple[int, bytes, bytes | None]

DEFAULT_CHECKPOINT_INTERVAL = 1000


async def scan_range(
    index: KeyIndex,
    state: dict[bytes, bytes],
    start: bytes | None = None,
    end: bytes | None = None,
) -> AsyncIterator[tuple[bytes, bytes]]:
    for key in index.range(start, end):
        yield key, state[key]


class Checkpoint(NamedTuple):
    seq: int
    state: dict[bytes, bytes]
    log_index: int


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
    def checkpoints(self) -> list[Checkpoint]: ...

    @abstractmethod
    def pinned_sequence(self) -> int: ...


class InMemoryStore(BaseStore):
    #: Key → value
    __d: dict[bytes, bytes]

    #: Sorted key index for range scans
    __index: KeyIndex

    #: Append-only write log (seq, key, val|None)
    __log: list[LogEntry]

    #: Materialized state snapshots in log order
    __checkpoints: list[Checkpoint]

    #: Write a checkpoint every N sequence numbers
    _checkpoint_interval: int

    def __init__(self, checkpoint_interval: int = DEFAULT_CHECKPOINT_INTERVAL):
        super().__init__()

        self.__d = {}
        self.__index = KeyIndex()
        self.__log = []
        self.__checkpoints = []
        self._checkpoint_interval = checkpoint_interval
        self._seq = SeqNum()

    def _maybe_checkpoint(self) -> None:
        if int(self._seq) % self._checkpoint_interval != 0:
            return
        self.__checkpoints.append(
            Checkpoint(int(self._seq), dict(self.__d), len(self.__log))
        )

    async def get(self, key: bytes) -> bytes:
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        return self.__d[key]

    async def put(self, key: bytes, val: bytes):
        self._seq.increment()
        self.__log.append((int(self._seq), key, val))
        if key not in self.__d:
            self.__index.insert(key)
        self.__d[key] = val
        self._maybe_checkpoint()

    async def delete(self, key: bytes):
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        self._seq.increment()
        self.__log.append((int(self._seq), key, None))
        del self.__d[key]
        self.__index.remove(key)
        self._maybe_checkpoint()

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

    def checkpoints(self) -> list[Checkpoint]:
        return self.__checkpoints

    def pinned_sequence(self) -> int:
        return int(self._seq)


class ObjectStore:
    @classmethod
    def resolve(cls, addr: str) -> BaseStore:
        if addr == ":memory:":
            return InMemoryStore()

        raise ValueError("Address format {} failed to resolve".format(addr))
