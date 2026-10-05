"""
test_lsmtree focuses on testing internals of the lsm as it acts
as an orchestration layer between in-memory and disk storage.
"""

import pytest

from cailloudb.custom_types import MemTableEntry
from cailloudb.lsm.lsmtree import LSMTree

__all__ = [
    "test_lsmtree_internals_iteratively_0_record_memtable",
    "test_lsmtree_internals_iteratively_1_records_memtables",
    "test_lsmtree_get_from_skiplist",
    "test_lsmtree_get_from_sstable_because_of_spill",
    "test_lsmtree_get_from_sstable_or_skiplist",
    "test_lsm_delete_from_sstables_or_skiplist",
    "test_lsmtree_scan_fixed_range",
    "test_lsmtree_scan_unbounded_range",
    "test_lsmtree_scan_max_seq",
    "test_lsmtree_scan_iterator_agg_break",
    "test_lsmtree_scan_future_seq_continue",
    "test_lsmtree_flush_empty_flush",
]


def test_lsmtree_internals_iteratively_0_record_memtable():
    lsmtree = LSMTree(10)  # should be full for each record

    lsmtree.put(b"key1", 0, b"val1")
    assert len(lsmtree.memtable) == 0
    assert len(lsmtree.sstables) == 1

    lsmtree.put(b"key2", 1, b"val2")
    assert len(lsmtree.memtable) == 0
    assert len(lsmtree.sstables) == 2
    assert len(lsmtree.sstables[0].offsets) == 1

    lsmtree.put(b"key3", 2, b"val3")
    assert len(lsmtree.memtable) == 0
    assert len(lsmtree.sstables) == 3
    assert len(lsmtree.sstables[0].offsets) == 1


def test_lsmtree_internals_iteratively_1_records_memtables():
    lsmtree = LSMTree(30)  # should hold only 1 record

    lsmtree.put(b"key1", 0, b"val1")
    assert len(lsmtree.memtable) == 1
    assert len(lsmtree.sstables) == 0

    lsmtree.put(b"key2", 1, b"val2")
    assert len(lsmtree.memtable) == 0
    assert len(lsmtree.sstables) == 1
    assert len(lsmtree.sstables[0].offsets) == 2

    lsmtree.put(b"key3", 2, b"val3")
    assert len(lsmtree.memtable) == 1
    assert len(lsmtree.sstables) == 1
    assert len(lsmtree.sstables[0].offsets) == 2


def test_lsmtree_get_from_skiplist():
    lsmtree = LSMTree()

    lsmtree.put(b"key1", 0, b"val1")

    value = lsmtree.get(b"key1", 0)

    assert value == b"val1"


def test_lsmtree_get_from_sstable_because_of_spill():
    lsmtree = LSMTree(10)  # should be full for each record

    lsmtree.put(b"key1", 0, b"val1")
    lsmtree.put(b"key2", 1, b"val2")
    lsmtree.put(b"key3", 2, b"val3")

    # Should get it from "oldest" sstable
    assert lsmtree.get(b"key1", 2) == b"val1"
    assert lsmtree.memtable.get(b"key1", 2) is None
    assert lsmtree.sstables[0].get(b"key1", 2) == MemTableEntry(
        key=b"key1", seq_num=0, value=b"val1"
    )

    # Should get it from "middle" sstable
    assert lsmtree.get(b"key2", 2) == b"val2"
    assert lsmtree.memtable.get(b"key2", 2) is None
    assert lsmtree.sstables[1].get(b"key2", 2) == MemTableEntry(
        key=b"key2", seq_num=1, value=b"val2"
    )

    # Should get it from "recent" sstable
    assert lsmtree.get(b"key3", 2) == b"val3"
    assert lsmtree.sstables[2].get(b"key3", 2) == MemTableEntry(
        key=b"key3", seq_num=2, value=b"val3"
    )


def test_lsmtree_get_from_sstable_or_skiplist():
    lsmtree = LSMTree(30)  # should hold only 1 record

    lsmtree.put(b"key1", 0, b"val1")
    lsmtree.put(b"key2", 1, b"val2")
    lsmtree.put(b"key3", 2, b"val3")

    # Each insertion immediately fills the active skiplist
    # SSTable are then flushed
    # and a fresh memtable is created
    assert lsmtree.memtable.bytes_size > 0
    assert len(lsmtree.sstables) == 1

    # Should get it from "oldest" sstable
    assert lsmtree.get(b"key1", 2) == b"val1"
    assert lsmtree.memtable.get(b"key1", 2) is None
    assert lsmtree.sstables[0].get(b"key1", 2) == MemTableEntry(
        key=b"key1", seq_num=0, value=b"val1"
    )

    # Should get it from "recent" sstable
    assert lsmtree.get(b"key2", 2) == b"val2"
    assert lsmtree.memtable.get(b"key2", 2) is None
    assert lsmtree.sstables[0].get(b"key2", 2) == MemTableEntry(
        key=b"key2", seq_num=1, value=b"val2"
    )

    # Should get it from skiplist
    assert lsmtree.get(b"key3", 2) == b"val3"
    assert lsmtree.memtable.get(b"key3", 2) == MemTableEntry(
        key=b"key3", seq_num=2, value=b"val3"
    )


def test_lsm_delete_from_sstables_or_skiplist():
    lsmtree = LSMTree(30)  # should hold only 1 record

    lsmtree.delete(b"key1", 0)
    lsmtree.delete(b"key2", 1)
    lsmtree.delete(b"key3", 2)

    assert lsmtree.get(b"key1", 2) is None
    assert lsmtree.get(b"key2", 2) is None
    assert lsmtree.get(b"key3", 2) is None


def test_lsmtree_scan_fixed_range():
    lsmtree = LSMTree(30)  # should hold only 1 record

    lsmtree.put(b"key1", 0, b"val1")
    lsmtree.put(b"key2", 1, b"val2")
    lsmtree.put(b"key3", 2, b"val3")

    # Test inclusive
    result = [item for item in lsmtree.scan(b"key", b"key4", 2)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
        (b"key3", b"val3"),
    ]

    # Test excluding start_key
    result = [item for item in lsmtree.scan(b"key2", b"key4", 2)]
    assert result == [
        (b"key2", b"val2"),
        (b"key3", b"val3"),
    ]

    # Test excluding end_key
    result = [item for item in lsmtree.scan(b"key1", b"key3", 2)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
    ]

    # Test excluding both
    result = [item for item in lsmtree.scan(b"key2", b"key3", 2)]
    assert result == [
        (b"key2", b"val2"),
    ]


def test_lsmtree_scan_unbounded_range():
    lsmtree = LSMTree(30)  # should hold only 1 record

    lsmtree.put(b"key1", 0, b"val1")
    lsmtree.put(b"key2", 1, b"val2")
    lsmtree.put(b"key3", 2, b"val3")

    # Test unbounded end_key
    result = [item for item in lsmtree.scan(b"key1", None, 2)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
        (b"key3", b"val3"),
    ]

    # Test unbounded start_key
    result = [item for item in lsmtree.scan(None, b"key4", 2)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
        (b"key3", b"val3"),
    ]

    # Test unbounded both
    result = [item for item in lsmtree.scan(None, None, 2)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
        (b"key3", b"val3"),
    ]


def test_lsmtree_scan_max_seq():
    lsmtree = LSMTree(30)  # should hold only 1 record

    lsmtree.put(b"key1", 0, b"val1")
    lsmtree.put(b"key2", 1, b"val2")
    lsmtree.put(b"key3", 2, b"val3")

    # Test fixed range
    result = [item for item in lsmtree.scan(b"key1", b"key4", 1)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
    ]

    # Test unbounded
    result = [item for item in lsmtree.scan(None, None, 1)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
    ]


def test_lsmtree_scan_iterator_agg_break(monkeypatch: pytest.MonkeyPatch):
    lsm = LSMTree()

    # Mock iterator_agg internal function from scan()
    mock_iterator = iter(
        [
            {"key": b"key1", "seq_num": 1, "value": b"val1"},
            {
                "key": b"key3",
                "seq_num": 2,
                "value": b"val3",
            },  # >= end_key, triggers `break`
            {
                "key": b"key4",
                "seq_num": 3,
                "value": b"val4",
            },  # Should never be processed
        ]
    )

    monkeypatch.setattr(
        lsm.memtable,
        "scan",
        lambda _start_key, _end_key, _seq_num: mock_iterator,
    )
    assert len(lsm.sstables) == 0

    # end_key=b"key2" means b"key3" will trigger the break condition
    result = list(lsm.scan(start_key=b"key0", end_key=b"key2", seq_num=5))

    # Only b"key1" should be yielded
    assert result == [(b"key1", b"val1")]


def test_lsmtree_scan_future_seq_continue(monkeypatch: pytest.MonkeyPatch):
    lsm = LSMTree()

    # Mock iterator_agg internal function from scan()
    mock_iterator = iter(
        [
            {
                "key": b"key1",
                "seq_num": 10,
                "value": b"future_val",
            },  # > seq_num, triggers `continue`
            {"key": b"key1", "seq_num": 5, "value": b"past_val"},  # Valid version
        ]
    )
    monkeypatch.setattr(
        lsm.memtable,
        "scan",
        lambda _start_key, _end_key, _seq_num: mock_iterator,
    )
    lsm.sstables = []

    # seq_num=7 means seq_num=10 is from the future and should be skipped
    result = list(lsm.scan(start_key=None, end_key=None, seq_num=7))

    # The future_val should be skipped, yielding only past_val
    assert result == [(b"key1", b"past_val")]


def test_lsmtree_flush_empty_flush(monkeypatch: pytest.MonkeyPatch):
    lsm = LSMTree()

    lsm._flush()

    assert len(lsm.sstables) == 0
