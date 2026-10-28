import asyncio

from cailloudb import DbBuilder, InMemoryStore, Settings


async def main():
    store = InMemoryStore("data/crud_in_memory")
    builder = DbBuilder(store, Settings(wal_enabled=True))
    db = builder.build()

    await db.put(b"entry1", b"value1")
    await db.put(b"entry2", b"value2")

    print((await db.get(b"entry1")).decode())
    print((await db.get(b"entry2")).decode())


asyncio.run(main())
