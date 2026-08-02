# CaillouDB

A very slow embedded key-valye store - single node - single-writer - object-storage backed.

# Features

- Async api
- Basic CRUD operations (in memory)
- Atomic batch write (in memory)

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

```mermaid
classDiagram
  class Db {
    store
    \_\_init\_\_(store: Store)
    delete(key: bytes)
    exists(key: bytes)
    get(key: bytes)
    put(key: bytes, value: bytes)
    snapshot() DbSnapshot
    shutdown()
  }
  class DbBuilder {
    name : str
    store
    \_\_init\_\_(name: str, store: Store)
    build() Db
  }
  class DbSnapshot {
    \_store
    \_seq : int
    \_\_init\_\_(store: Store)
    exists(key: bytes) bool
    get(key: bytes) bytes
    latest_sequence_number() int
    scan(start: bytes, end: bytes)
  }
  class InMemoryStore {
    \_\_d : dict[bytes, bytes]
    \_\_init\_\_()
    delete(key: bytes)
    exists(key: bytes) bool
    get(key: bytes) bytes
    put(key: bytes, val: bytes)
  }
  class ObjectStore {
    resolve(addr: str) Store
  }
  class Store {
    delete(key: bytes)*
    exists(key: bytes)* bool
    get(key: bytes)* bytes
    put(key: bytes, val: bytes)*
  }
  InMemoryStore --|> Store
  Store --o Db : store
  Store --o DbBuilder : store
  Store --o DbSnapshot : store

```
