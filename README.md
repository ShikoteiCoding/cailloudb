# CaillouDB

A very slow key-valye store - single node - object-storage backed.

# Features

- Async api
- Basic CRUD operations

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
    delete(key)
    exists(key)
    get(key)
    put(key, value)
  }
  class DbBuilder {
    path : str
    store : str
    \_\_init\_\_(path: str)
    build() Db
  }
  class InMemoryStore {
    \_\_d : dict
    \_\_init\_\_()
    delete(key)
    exists(key) bool
    get(key)
    put(key, value)
  }
  class Store {
    delete(key)*
    exists(key)* bool
    get(key)* Any
    put(key, val)*
  }
  class Store {
    delete(key)*
    exists(key)* bool
    get(key)* Any
    put(key, val)*
  }
  InMemoryStore --|> Store
  Store --o Db : store
```