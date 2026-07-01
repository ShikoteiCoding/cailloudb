from store import Store


class Db:
    def __init__(self, storage: Store):
        self.storage = storage

    def get(self, key):
        return self.storage.get(key)

    def put(self, key, value):
        self.storage.put(key, value)

    def delete(self, key):
        self.storage.delete(key)

    def exists(self, key):
        return self.storage.exists(key)
