from store import Store


class Db:
    store: Store

    def __init__(self, store: Store):
        self.store = store

    async def get(self, key: bytes):
        return await self.store.get(key)

    async def put(self, key: bytes, value: bytes):
        await self.store.put(key, value)

    async def delete(self, key: bytes):
        await self.store.delete(key)

    async def exists(self, key: bytes):
        return await self.store.exists(key)

    async def shutdown(self): ...
