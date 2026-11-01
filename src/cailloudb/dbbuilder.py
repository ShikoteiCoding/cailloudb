from typing import TYPE_CHECKING

from db import Db
from settings import Settings

if TYPE_CHECKING:
    from store import BaseStore


class DbBuilder:
    #: Store ref
    store: BaseStore

    #: Runtime settings
    _settings: Settings

    def __init__(self, store: BaseStore, settings: Settings | None = None):
        self.store = store
        self._settings = settings or Settings()

    def build(self) -> Db:
        return Db(self.store)
