from typing import TYPE_CHECKING

from write_batch import WriteBatch

if TYPE_CHECKING:
    from db import Db


class Transaction:
    _db: Db
    _batch: WriteBatch
    _committed: bool

    def __init__(self, db: Db):
        self._db = db
        self._batch = WriteBatch()
        self._committed = False

    async def put(self, key: bytes, val: bytes) -> None:
        self._batch.put(key, val)

    async def delete(self, key: bytes) -> None:
        self._batch.delete(key)

    async def commit(self) -> None:
        if self._committed:
            raise RuntimeError("transaction already committed")

        await self._db.write(self._batch)
        self._committed = True
