import pytest

from cailloudb.custom_types import MemTableEntry
from cailloudb.lsm.lsmtree import LSMTree


def test_lsmtree_get_from_skiplist():
    lsmtree = LSMTree()

    lsmtree.put(b"key1", 0, b"val1")

    value = lsmtree.get(b"key1")

    assert value == b"val1"


def test_lsmtree_get_from_sstable_because_of_spill():
    # Make skiplist hold max 1 record before being full
    lsmtree = LSMTree(10)
    assert lsmtree.memtable.bytes_size == 0
    assert len(lsmtree.sstables) == 0

    lsmtree.put(b"key1", 0, b"val1")
    lsmtree.put(b"key2", 1, b"val2")
    lsmtree.put(b"key3", 2, b"val3")

    # Each insertion immediatelu fills the active skiplist
    # SSTable are then flushed
    # and a fresh memtable is created
    assert lsmtree.memtable.bytes_size == 0
    assert len(lsmtree.sstables) == 3

    # Should get it from "oldest" sstable
    assert lsmtree.get(b"key1") == b"val1"
    assert lsmtree.memtable.get(b"key1") is None
    assert lsmtree.sstables[0].get(b"key1") == MemTableEntry(
        key=b"key1", seq_num=0, value=b"val1"
    )

    # Should get it from "middle" sstable
    assert lsmtree.get(b"key2") == b"val2"
    assert lsmtree.memtable.get(b"key2") is None
    assert lsmtree.sstables[1].get(b"key2") == MemTableEntry(
        key=b"key2", seq_num=1, value=b"val2"
    )

    # Should get it from "recent" sstable
    assert lsmtree.get(b"key3") == b"val3"
    assert lsmtree.sstables[2].get(b"key3") == MemTableEntry(
        key=b"key3", seq_num=2, value=b"val3"
    )


def test_lsmtree_get_from_sstable_and_skiplist():
    # Make skiplist hold max 2 records before being full
    lsmtree = LSMTree(30)
    assert lsmtree.memtable.bytes_size == 0
    assert len(lsmtree.sstables) == 0

    lsmtree.put(b"key1", 0, b"val1")
    lsmtree.put(b"key2", 1, b"val2")
    lsmtree.put(b"key3", 2, b"val3")

    # Each insertion immediatelu fills the active skiplist
    # SSTable are then flushed
    # and a fresh memtable is created
    assert lsmtree.memtable.bytes_size > 0
    assert len(lsmtree.sstables) == 1

    # Should get it from "oldest" sstable
    val1 = lsmtree.get(b"key1")
    assert val1 == b"val1"
    assert lsmtree.memtable.get(b"key1") is None
    assert lsmtree.sstables[0].get(b"key1") == MemTableEntry(
        key=b"key1", seq_num=0, value=b"val1"
    )

    # Should get it from "recent" sstable
    assert lsmtree.get(b"key2") == b"val2"
    assert lsmtree.memtable.get(b"key2") is None
    assert lsmtree.sstables[0].get(b"key2") == MemTableEntry(
        key=b"key2", seq_num=1, value=b"val2"
    )

    # Should get it from skiplist
    assert lsmtree.get(b"key3") == b"val3"
    assert lsmtree.memtable.get(b"key3") == MemTableEntry(
        key=b"key3", seq_num=2, value=b"val3"
    )
