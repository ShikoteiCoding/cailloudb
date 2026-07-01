from db import Db
from store import Store, InMemoryStore


class DbBuilder:
    storage: Store

    def __init__(self, path: str = ":memory:"):
        if path == ":memory:":
            self.storage = InMemoryStore()

    def build(self) -> Db:
        db = Db(self.storage)
        return db
