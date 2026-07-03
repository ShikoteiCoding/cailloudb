import asyncio

from cailloudb import ObjectStore, DbBuilder, WriteBatch


async def main():
    store = ObjectStore.resolve(":memory:")
    builder = DbBuilder("", store)
    db = builder.build()

    batch = WriteBatch()
    batch.put(b"entry1", b"value1")
    batch.put(b"entry2", b"value2")
    batch.delete(b"entry1")
    batch.put(b"entry1", b"value1-bis")

    print(len(batch))

    await db.write(batch)


asyncio.run(main())
