from unittest.mock import patch

import pytest

from cailloudb.custom_types import MemTableEntry
from cailloudb.lsm.skiplist import SkipList


@pytest.fixture
def skiplist() -> SkipList:
    return SkipList(max_level=3, p=0.5)


def test_skiplist_initial_state(skiplist: SkipList):
    assert skiplist.level == 0
    assert skiplist._size == 0
    assert len(skiplist.header.forward) == skiplist.max_level + 1
    assert all(ptr is None for ptr in skiplist.header.forward)


def test_skiplist_insert_and_get(skiplist: SkipList):
    skiplist.insert(b"key1", 0, b"val1")
    initial_level = skiplist.level
    initial_size = skiplist._size

    assert len(skiplist) == 1 == initial_size
    assert skiplist.level == initial_level

    assert skiplist.get(b"key1", 0) == MemTableEntry(
        key=b"key1", seq_num=0, value=b"val1"
    )
    assert skiplist.get(b"key2", 0) is None


def test_skiplist_insert_same_key(skiplist: SkipList):
    skiplist.insert(b"key1", 0, b"val1")
    initial_level = skiplist.level
    initial_size = skiplist._size

    assert len(skiplist) == 1 == initial_size
    assert skiplist.level == initial_level

    skiplist.insert(b"key1", 1, b"val2")

    # Size has increased because of insert
    assert len(skiplist) == 2 == initial_size + 1
    assert skiplist.get(b"key1", 1) == MemTableEntry(
        key=b"key1", seq_num=1, value=b"val2"
    )


@patch.object(SkipList, "_random_level", return_value=0)
def test_skiplist_insert_new_node_level_0(mock_random, skiplist: SkipList):
    """
    Test inserting a node that gets assigned level 0 each time.
    """
    skiplist.insert(b"key1", 0, b"val1")

    assert skiplist._size == 1
    assert skiplist.level == 0

    # Verify internal pointers
    node = skiplist.header.forward[0]
    assert node is not None
    assert node.key == b"key1"
    assert node.value == b"val1"
    assert node.seq_num == 0

    # The node's forward pointer should be None
    assert node.forward[0] is None
    # Higher levels in the header should still be None
    assert skiplist.header.forward[1] is None


@patch.object(SkipList, "_random_level", return_value=2)
def test_skiplist_level_expansion(mock_random, skiplist: SkipList):
    """Test scenario where new level is assigned"""
    skiplist.insert(b"key1", 0, b"val1")

    # SkipList level should expand to 2
    assert len(skiplist) == 1
    assert skiplist.level == 2

    # Header pointers 0, 1, and 2 all point to the new node
    node = skiplist.header.forward[0]
    assert skiplist.header.forward[1] is node
    assert skiplist.header.forward[2] is node

    # Level 3 is still None
    assert skiplist.header.forward[3] is None

    # Level 4 is not created yet
    with pytest.raises(IndexError):
        skiplist.header.forward[4]


@patch.object(SkipList, "_random_level", side_effect=[0, 2, 1, 0])
def test_skiplist_complex_pointer_routing(mock_random, skiplist: SkipList):
    """
    Test a "random" scenario across multiple levels.
    Insert sequence: A (lvl 0), B (lvl 2), C (lvl 1), D (lvl 0)

    Expected structure:
    L2: Header -> B -> None
    L1: Header -> B -> C -> None
    L0: Header -> A -> B -> C -> D -> None
    """
    skiplist.insert(b"A", 0, b"valA")
    skiplist.insert(b"B", 1, b"valB")
    skiplist.insert(b"C", 2, b"valC")
    skiplist.insert(b"D", 3, b"valD")

    assert skiplist._size == 4
    assert skiplist.level == 2

    # Take each creaed node
    node_a = skiplist.header.forward[0]
    node_b = node_a.forward[0]
    node_c = node_b.forward[0]
    node_d = node_c.forward[0]

    assert node_a.key == b"A"
    assert node_b.key == b"B"
    assert node_c.key == b"C"
    assert node_d.key == b"D"

    # Check forward refs from level 2
    assert skiplist.header.forward[2] is node_b
    assert node_b.forward[2] is None

    # Check forward refs from level 1
    assert skiplist.header.forward[1] is node_b
    assert node_b.forward[1] is node_c
    assert node_c.forward[1] is None

    # Check forward refs from level 0
    assert node_d.forward[0] is None


@patch.object(SkipList, "_random_level", return_value=1)
def test_skiplist_put_out_of_order_insertion(mock_random, skiplist: SkipList):
    """
    Test that nodes inserted in reverse order still preserve key ordering.
    """
    skiplist.insert(b"Z", 0, b"valZ")
    skiplist.insert(b"M", 1, b"valM")
    skiplist.insert(b"A", 2, b"valA")

    # L0 traversal should yield A -> M -> Z
    curr = skiplist.header.forward[0]
    assert curr.key == b"A"

    curr = curr.forward[0]
    assert curr.key == b"M"

    curr = curr.forward[0]
    assert curr.key == b"Z"

    # Z should terminate the list
    assert curr.forward[0] is None


def test_skiplist_scan_fixed_range(skiplist: SkipList):
    skiplist.insert(b"key1", 0, b"val1")
    skiplist.insert(b"key2", 1, b"val2")
    skiplist.insert(b"key3", 2, b"val3")

    # Test inclusive
    result = [item for item in skiplist.scan(b"key", b"key4", 2)]
    assert result == [
        MemTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        MemTableEntry(key=b"key2", seq_num=1, value=b"val2"),
        MemTableEntry(key=b"key3", seq_num=2, value=b"val3"),
    ]

    # Test excluding start_key
    result = [item for item in skiplist.scan(b"key2", b"key4", 2)]
    assert result == [
        MemTableEntry(key=b"key2", seq_num=1, value=b"val2"),
        MemTableEntry(key=b"key3", seq_num=2, value=b"val3"),
    ]

    # Test excluding end_key
    result = [item for item in skiplist.scan(b"key1", b"key3", 2)]
    assert result == [
        MemTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        MemTableEntry(key=b"key2", seq_num=1, value=b"val2"),
    ]

    # Test excluding both
    result = [item for item in skiplist.scan(b"key2", b"key3", 2)]
    assert result == [
        MemTableEntry(key=b"key2", seq_num=1, value=b"val2"),
    ]


def test_skiplist_scan_unbounded_range(skiplist: SkipList):
    skiplist.insert(b"key1", 0, b"val1")
    skiplist.insert(b"key2", 1, b"val2")
    skiplist.insert(b"key3", 2, b"val3")

    # Test unbounded end_key
    result = [item for item in skiplist.scan(b"key1", None, 2)]
    assert result == [
        MemTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        MemTableEntry(key=b"key2", seq_num=1, value=b"val2"),
        MemTableEntry(key=b"key3", seq_num=2, value=b"val3"),
    ]

    # Test unbounded start_key
    result = [item for item in skiplist.scan(None, b"key4", 2)]
    assert result == [
        MemTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        MemTableEntry(key=b"key2", seq_num=1, value=b"val2"),
        MemTableEntry(key=b"key3", seq_num=2, value=b"val3"),
    ]

    # Test unbounded both
    result = [item for item in skiplist.scan(None, None, 2)]
    assert result == [
        MemTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        MemTableEntry(key=b"key2", seq_num=1, value=b"val2"),
        MemTableEntry(key=b"key3", seq_num=2, value=b"val3"),
    ]


def test_skiplist_scan_max_seq(skiplist: SkipList):
    skiplist.insert(b"key1", 0, b"val1")
    skiplist.insert(b"key2", 1, b"val2")
    skiplist.insert(b"key3", 2, b"val3")

    # Test fixed range
    result = [item for item in skiplist.scan(b"key1", b"key4", 1)]
    assert result == [
        MemTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        MemTableEntry(key=b"key2", seq_num=1, value=b"val2"),
    ]

    # Test unbounded
    result = [item for item in skiplist.scan(None, None, 1)]
    assert result == [
        MemTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        MemTableEntry(key=b"key2", seq_num=1, value=b"val2"),
    ]
