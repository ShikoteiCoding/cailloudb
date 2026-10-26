from typing import TYPE_CHECKING

from settings import Settings
from db import Db

if TYPE_CHECKING:
    from store import BaseStore


class DbBuilder:
    #: Name of the Db KV wrapper
    name: str

    #: Store ref
    store: BaseStore

    #: Runtime settings
    settings: Settings

    def __init__(
        self,
        name: str,
        store: BaseStore,
        settings: Settings | None = None,
    ):
        self.name = name
        self.store = store
        self.settings = settings or Settings()

    def build(self) -> Db:
        return Db(self.store)
