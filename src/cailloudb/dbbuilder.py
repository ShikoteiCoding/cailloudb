from typing import TYPE_CHECKING

from db import Db

if TYPE_CHECKING:
    from store import BaseStore


class DbBuilder:
    #: Name of the Db KV wrapper
    name: str

    #: Store ref
    store: BaseStore

    def __init__(self, name: str, store: BaseStore):
        self.name = name
        self.store = store

    def build(self) -> Db:
        return Db(self.store)
