from typing import TYPE_CHECKING, AsyncIterator

if TYPE_CHECKING:
    from store import BaseStore


class DbReader:
    """Read-only in-process"""

    _store: BaseStore

    def __init__(self, store: BaseStore):
        self._store = store

    async def get(self, key: bytes) -> bytes:
        return await self._store.get(key)

    async def exists(self, key: bytes) -> bool:
        return await self._store.exists(key)

    def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        return self._store.scan(start, end)

    async def latest_sequence_number(self) -> int:
        return await self._store.latest_sequence_number()
