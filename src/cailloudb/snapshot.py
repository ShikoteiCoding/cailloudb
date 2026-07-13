from typing import TYPE_CHECKING, AsyncIterator

from index import KeyIndex
from store import LogEntry, SeqNum

if TYPE_CHECKING:
    from collections.abc import Sequence
    from store import BaseStore


def replay_log(log: Sequence[LogEntry], max_seq: SeqNum) -> dict[bytes, bytes]:
    state: dict[bytes, bytes] = {}
    limit = int(max_seq)
    for seq, key, val in log:
        if int(seq) > limit:
            break
        if val is None:
            state.pop(key, None)
        else:
            state[key] = val
    return state


class DbSnapshot:
    """Read-only point-in-time view pinned to a sequence number."""

    _log: list[LogEntry]
    _seq: SeqNum
    _state: dict[bytes, bytes] | None
    _index: KeyIndex | None

    def __init__(self, store: "BaseStore"):
        self._log = store.write_log()
        self._seq = store.pinned_sequence()
        self._state = None
        self._index = None

    def seq(self) -> int:
        return int(self._seq)

    def _materialize(self) -> dict[bytes, bytes]:
        if self._state is None:
            self._state = replay_log(self._log, self._seq)
            self._index = KeyIndex()
            for key in self._state:
                self._index.insert(key)
        return self._state

    async def get(self, key: bytes) -> bytes:
        state = self._materialize()
        if key not in state:
            raise KeyError("key {} not found".format(key))
        return state[key]

    async def exists(self, key: bytes) -> bool:
        return key in self._materialize()

    def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        state = self._materialize()
        index = self._index
        assert index is not None

        async def _iter():
            for key in index.range(start, end):
                yield key, state[key]

        return _iter()
