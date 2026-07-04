from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from store import BaseStore
    from write_batch import WriteBatch


class Db:
    #: Actual KV Store
    store: BaseStore

    def __init__(self, store: BaseStore):
        self.store = store

    async def get(self, key: bytes):
        return await self.store.get(key)

    async def put(self, key: bytes, val: bytes):
        await self.store.put(key, val)

    async def delete(self, key: bytes):
        await self.store.delete(key)

    async def exists(self, key: bytes):
        return await self.store.exists(key)

    async def shutdown(self): ...

    async def write(self, batch: WriteBatch):
        await self.store.write(batch)
