from cailloudb.constants import LEN_METADATA, TOMBSTONE
from cailloudb.custom_types import MemTableEntry
from cailloudb.lsm.memtable import MemTable

__all__ = [
    "test_memtable_insert_and_get",
    "test_memtable_insert_tombstone",
    "test_memtable_full",
]


def test_memtable_insert_and_get():
    memtable = MemTable()

    memtable.insert(b"key1", 0, b"val1")

    assert memtable.bytes_size == len(b"key1") + len(b"val1") + LEN_METADATA
    assert len(memtable) == 1
    assert memtable.get(b"key1", 0) == MemTableEntry(
        key=b"key1", seq_num=0, value=b"val1"
    )
    assert memtable.get(b"key2", 0) is None


def test_memtable_insert_tombstone():
    memtable = MemTable()

    memtable.insert(b"key1", 0, b"val1")

    assert memtable.bytes_size == len(b"key1") + len(b"val1") + LEN_METADATA
    assert len(memtable) == 1
    assert memtable.get(b"key1", 0) == MemTableEntry(
        key=b"key1", seq_num=0, value=b"val1"
    )
    assert memtable.get(b"key2", 0) is None

    memtable.insert(b"key1", 0, TOMBSTONE)
    assert len(memtable) == 2
    assert memtable.bytes_size == (
        len(b"key1")
        + LEN_METADATA
        + len(b"val1")
        + len(b"key1")
        + LEN_METADATA
        + len(TOMBSTONE)
    )
    assert memtable.get(b"key1", 0) == MemTableEntry(
        key=b"key1", seq_num=0, value=TOMBSTONE
    )


def test_memtable_full():
    memtable = MemTable(max_bytes_size=1)
    assert not memtable.is_full()

    memtable.insert(b"key1", 0, b"val1")
    assert memtable.is_full()
