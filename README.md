# CaillouDB

A very slow embedded key-valye store API - single node - single-writer. Aim to be externally managed, just gives the right APIs.

# Features

- Async api
- Basic CRUD operations (in memory)
- Atomic batch write (in memory)
- Disk-backed (to come)
- Disk WAL (to come)
- LSM-tree (to come)
- Compaction API (to come)
- Multi-tiers API (to come)

# Quickstart
```python
import asyncio

from cailloudb import (
    ObjectStore,
    DbBuilder
)

async def main():
    store = ObjectStore.resolve(":memory:")
    builder = DbBuilder("test-db", store)
    db = builder.build()

    await db.put(b"entry1", b"value1")
    await db.put(b"entry2", b"value2")

    print(await db.get(b"entry1"))
    print(await db.get(b"entry2"))


asyncio.run(main())
```

# Class Diagram

See `src/classes.mmd`
