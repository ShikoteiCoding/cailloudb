from typing import TYPE_CHECKING

from db import Db

if TYPE_CHECKING:
    from store import Store


class DbBuilder:
    name: str
    store: Store

    def __init__(self, name: str, store: Store):
        self.name = name
        self.store = store

    def build(self) -> Db:
        return Db(self.store)
