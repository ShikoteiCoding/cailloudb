import bisect
from typing import TYPE_CHECKING, AsyncIterator

from index import KeyIndex
from store import BaseStore, Checkpoint, LogEntry

if TYPE_CHECKING:
    from collections.abc import Sequence


class DbSnapshot:
    """Read-only point-in-time view pinned to a sequence number."""

    #: Pinned sequence number at snapshot time
    _seq: int

    #: Materialized key → value at pinned seq
    __d: dict[bytes, bytes]

    #: Sorted key index for range scans
    __index: KeyIndex

    def __init__(self, store: BaseStore):
        self._seq = store.pinned_sequence()
        self.__d = self.replay_log(
            store.write_log(), self._seq, store.checkpoints()
        )
        self.__index = KeyIndex.from_keys(self.__d)

    def seq(self) -> int:
        return self._seq

    @staticmethod
    def find_checkpoint(
        checkpoints: Sequence[Checkpoint], max_seq: int
    ) -> Checkpoint | None:
        """Nearest checkpoint at or before max_seq."""
        if not checkpoints:
            return None
        seqs = [cp.seq for cp in checkpoints]
        idx = bisect.bisect_right(seqs, max_seq) - 1
        if idx < 0:
            return None
        return checkpoints[idx]

    @staticmethod
    def replay_log(
        log: Sequence[LogEntry],
        max_seq: int,
        checkpoints: Sequence[Checkpoint] = (),
    ) -> dict[bytes, bytes]:
        """Rebuild key → value state from log entries up to max_seq."""
        checkpoint = DbSnapshot.find_checkpoint(checkpoints, max_seq)
        if checkpoint is None:
            state: dict[bytes, bytes] = {}
            start = 0
        else:
            state = dict(checkpoint.state)
            start = checkpoint.log_index

        for seq, key, val in log[start:]:
            if seq > max_seq:
                break
            if val is None:
                state.pop(key, None)
            else:
                state[key] = val
        return state

    async def get(self, key: bytes) -> bytes:
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        return self.__d[key]

    async def exists(self, key: bytes) -> bool:
        return key in self.__d

    async def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        for key in self.__index.range(start, end):
            yield key, self.__d[key]
