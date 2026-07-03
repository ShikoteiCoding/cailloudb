from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from store import Store
    from write_batch import WriteBatch


class Db:
    store: Store

    def __init__(self, store: Store):
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
        for key, val in batch:
            if val:
                await self.put(key, val)
            else:
                await self.delete(key)
