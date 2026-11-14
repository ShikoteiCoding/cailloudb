from typing import TYPE_CHECKING, AsyncIterator

if TYPE_CHECKING:
    from lsm.engine_snapshot import EngineSnapshot
    from store import BaseStore


class DbSnapshot:
    """Read-only point-in-time view pinned to a sequence number."""

    #: Store pointer
    _store: BaseStore

    #: Pinned Engine Snapshot
    _engine_snapshot: EngineSnapshot

    def __init__(self, store: BaseStore, engine_snapshot: EngineSnapshot):
        self._store = store
        self._engine_snapshot = engine_snapshot

    async def get(self, key: bytes) -> bytes | None:
        return await self._store.get(key, self._engine_snapshot)

    def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        return self._store.scan(start, end, self._engine_snapshot)

    def get_sequence_number(self) -> int:
        return self._engine_snapshot.get_pinned_sequence_number()
