from store import Store


class Db:
    store: Store

    def __init__(self, store: Store):
        self.store = store

    def get(self, key):
        return self.store.get(key)

    def put(self, key, value):
        self.store.put(key, value)

    def delete(self, key):
        self.store.delete(key)

    def exists(self, key):
        return self.store.exists(key)
