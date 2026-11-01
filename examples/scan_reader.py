import asyncio

from cailloudb import DbBuilder, InMemoryStore, WriteBatch


async def main():
    store = InMemoryStore("data/range_in_memory")
    db = DbBuilder(store).build()

    batch = WriteBatch()

    for i in range(0, 20):
        batch.put(f"{i}".encode(), f"value{i}".encode())

    print(f"Size of the batch: {len(batch)}")

    await db.write(batch)

    async for k, v in db.scan(bytes(10)):
        print(k.decode(), v.decode())


asyncio.run(main())
