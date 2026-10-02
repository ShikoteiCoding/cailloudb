from cailloudb.constants import TOMBSTONE
from cailloudb.custom_types import MemTableEntry
from cailloudb.lsm.memtable import MemTable
from cailloudb.lsm.skiplist import LEN_SEQUENCE_NUM


def test_memtable_put_and_get():
    memtable = MemTable()

    memtable.put(b"key1", 0, b"val1")

    assert memtable.bytes_size == len(b"key1") + len(b"val1") + LEN_SEQUENCE_NUM
    assert len(memtable) == 1
    assert memtable.get(b"key1") == MemTableEntry(key=b"key1", seq_num=0, value=b"val1")
    assert memtable.get(b"key2") is None


def test_memtable_put_and_delete():
    memtable = MemTable()

    memtable.put(b"key1", 0, b"val1")

    assert memtable.bytes_size == len(b"key1") + len(b"val1") + LEN_SEQUENCE_NUM
    assert len(memtable) == 1
    assert memtable.get(b"key1") == MemTableEntry(key=b"key1", seq_num=0, value=b"val1")
    assert memtable.get(b"key2") is None

    memtable.delete(b"key1", 0)
    assert len(memtable) == 2
    assert memtable.bytes_size == (
        len(b"key1")
        + LEN_SEQUENCE_NUM
        + len(b"val1")
        + len(b"key1")
        + LEN_SEQUENCE_NUM
        + len(TOMBSTONE)
    )
    assert memtable.get(b"key1") == MemTableEntry(
        key=b"key1", seq_num=0, value=TOMBSTONE
    )


def test_memtable_full():
    memtable = MemTable(max_bytes_size=1)
    assert not memtable.is_full()

    memtable.put(b"key1", 0, b"val1")
    assert memtable.is_full()
