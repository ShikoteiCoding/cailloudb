from db import Db
from store import Store, InMemoryStore


class DbBuilder:
    store: str

    def __init__(self, path: str = ":memory:"):
        self.path = path

    def build(self) -> Db:
        if self.path == ":memory:":
            db = Db(InMemoryStore())
            return db
        
        raise ValueError("Path {} not valid".format(self.path))
