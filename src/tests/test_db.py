import pytest

from cailloudb.dbbuilder import DbBuilder


def test_store_get_or_raise():
    db = DbBuilder().build()

    db.put(b"test1", b"val1")
    assert db.get(b"test1") == b"val1"

    db.put(b"test2", b"val2")
    assert db.get(b"test2") == b"val2"

    with pytest.raises(KeyError):
        db.get(b"test3")


def test_store_delete_or_raise():
    db = DbBuilder().build()

    db.put(b"test1", b"val1")
    db.put(b"test2", b"val2")

    db.delete(b"test1")
    with pytest.raises(KeyError):
        db.get(b"test1")

    with pytest.raises(KeyError):
        db.delete(b"test1")


def test_store_exist():
    db = DbBuilder().build()

    db.put(b"test1", b"val1")

    assert db.exists(b"test1")
    assert not db.exists(b"test2")
