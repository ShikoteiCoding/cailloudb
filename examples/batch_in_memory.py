import asyncio

from cailloudb import DbBuilder, InMemoryStore, WriteBatch


async def main():
    store = InMemoryStore()
    builder = DbBuilder("test-db", store)
    db = builder.build()

    batch = WriteBatch()
    batch.put(b"entry1", b"value1")
    batch.put(b"entry2", b"value2")
    batch.delete(b"entry1")
    batch.put(b"entry1", b"value1-bis")

    print(f"Size of the batch: {len(batch)}")

    await db.write(batch)


asyncio.run(main())
