import pytest

from store import InMemoryStore


def test_store_get_or_raise():
    store = InMemoryStore()

    store.put(b"test1", b"val1")
    assert store.get(b"test1") == b"val1"

    store.put(b"test2", b"val2")
    assert store.get(b"test2") == b"val2"

    with pytest.raises(KeyError):
        store.get(b"test3")


def test_store_delete_or_raise():
    store = InMemoryStore()

    store.put(b"test1", b"val1")
    store.put(b"test2", b"val2")

    store.delete(b"test1")
    with pytest.raises(KeyError):
        store.get(b"test1")

    with pytest.raises(KeyError):
        store.delete(b"test1")


def test_store_exist():
    store = InMemoryStore()

    store.put(b"test1", b"val1")

    assert store.exists(b"test1")
    assert not store.exists(b"test2")
