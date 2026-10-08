from typing import TYPE_CHECKING, AsyncIterator

if TYPE_CHECKING:
    from store import BaseStore


class DbSnapshot:
    """Read-only point-in-time view pinned to a sequence number."""

    #: Store pointer
    _store: BaseStore

    #: Pinned sequence number
    _seq: int

    def __init__(self, store: BaseStore):
        self._store = store
        self._seq = int(store._seq) - 1

    async def get(self, key: bytes) -> bytes:
        return await self._store.get_at(key, seq_num=self._seq)

    def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        return self._store.scan_at(start, end, seq_num=self._seq)

    async def latest_sequence_number(self) -> int:
        return self._seq
