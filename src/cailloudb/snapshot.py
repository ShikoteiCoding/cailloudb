from typing import TYPE_CHECKING, AsyncIterator

if TYPE_CHECKING:
    from store import BaseStore


class DbSnapshot:
    """Read-only point-in-time view pinned to a sequence number."""

    #: Live store — reads resolve at pinned seq via versioned values
    _store: BaseStore

    #: Sequence number captured at snapshot time
    _seq: int

    def __init__(self, store: BaseStore):
        self._store = store
        self._seq = int(store._seq)

    async def get(self, key: bytes) -> bytes:
        return await self._store.get_at(key, self._seq)

    async def exists(self, key: bytes) -> bool:
        return await self._store.exists_at(key, self._seq)

    def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        return self._store.scan_at(self._seq, start, end)

    async def latest_sequence_number(self) -> int:
        return self._seq
