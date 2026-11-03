from typing import TYPE_CHECKING, AsyncIterator

from dbreader import DbReader
from snapshot import DbSnapshot

if TYPE_CHECKING:
    from store import BaseStore
    from write_batch import WriteBatch


class Db:
    #: Actual KV Store
    store: BaseStore

    def __init__(self, store: BaseStore):
        self.store = store

    def reader(self) -> DbReader:
        return DbReader(self.store)

    def snapshot(self) -> DbSnapshot:
        return DbSnapshot(self.store)

    async def get(self, key: bytes) -> bytes | None:
        return await self.store.get(key)

    async def put(self, key: bytes, val: bytes) -> None:
        await self.store.put(key, val)

    async def delete(self, key: bytes) -> None:
        await self.store.delete(key)

    async def shutdown(self): ...

    async def write(self, batch: WriteBatch) -> None:
        await self.store.write(batch)

    async def latest_sequence_number(self) -> int:
        return await self.store.latest_sequence_number()

    def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        return self.store.scan(start, end)
