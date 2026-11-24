from typing import TYPE_CHECKING

from db import Db
from settings import Settings

if TYPE_CHECKING:
    from store import BaseStore


class DbBuilder:
    #: Store ref
    store: BaseStore

    def __init__(self, store: BaseStore):
        self.store = store

    def build(self) -> Db:
        return Db(self.store)
