from cailloudb.custom_types import TOMBSTONE
from cailloudb.lsm.memtable import MemTable


def test_memtable_put_and_get():
    memtable = MemTable()

    memtable.put(b"key1", b"val1")

    assert len(memtable) == 1
    assert memtable.bytes_size == len(b"key1") + len(b"val1")
    assert memtable.get(b"key1") == b"val1"
    assert memtable.get(b"key2") is None


def test_memtable_put_and_delete():
    memtable = MemTable()

    memtable.put(b"key1", b"val1")

    assert len(memtable) == 1
    assert memtable.bytes_size == len(b"key1") + len(b"val1")
    assert memtable.get(b"key1") == b"val1"
    assert memtable.get(b"key2") is None

    memtable.delete(b"key1")
    # Deletes for now just replace value with deletion marker
    assert len(memtable) == 1
    assert memtable.bytes_size == len(b"key1") + len(TOMBSTONE)
    assert memtable.get(b"key1") == TOMBSTONE


def test_memtable_full():
    memtable = MemTable(max_bytes_size=1)
    assert not memtable.is_full()

    memtable.put(b"key1", b"val1")
    assert memtable.is_full()
