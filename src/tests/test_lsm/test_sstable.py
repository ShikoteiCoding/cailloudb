from pathlib import Path

import pytest

from cailloudb.constants import TOMBSTONE
from cailloudb.custom_types import SSTableEntry
from cailloudb.lsm.memtable import MemTable
from cailloudb.lsm.sstable import (
    InMemorySSTable,
    SSTable,
    SSTableWriter,
)
from cailloudb.lsm.utils import extract_from_internal_key

__all__ = [
    "test_sstable_writer_write_sstable",
    "test_sstable_writer_to_disk",
    "test_sstable_get",
    "test_sstable_iter",
    "test_sstable_scan_fixed_range",
    "test_sstable_scan_unbounded_range",
    "test_sstable_scan_max_seq",
    "test_sstable_scan_all_occurence_of_a_key",
]


@pytest.fixture
def sstable_writer() -> SSTableWriter:
    return SSTableWriter(in_memory=True, dir=Path("/tmp/data"))


@pytest.fixture
def memtable() -> MemTable:
    return MemTable()


def test_sstable_writer_write_sstable(
    sstable_writer: SSTableWriter, memtable: MemTable
):
    memtable.insert(key=b"key1", seq_num=0, value=b"val1")
    memtable.insert(key=b"key2", seq_num=1, value=b"val2")
    memtable.insert(key=b"key3", seq_num=2, value=TOMBSTONE)

    sstable = sstable_writer.write(memtable, 1)[0]

    # Check sparse index
    assert len(sstable.index_keys) == 1  # 1 block ~4KB holds the 2 records

    # Check files
    assert sstable.file.tell() == 0
    assert len(sstable.file.getvalue()) > 0

    # Check data
    ssentries = [ssentry for ssentry in sstable]
    assert len(ssentries) == 3
    assert sstable.get(b"key1", 3) == SSTableEntry(
        key=b"key1", seq_num=0, value=b"val1"
    )
    assert sstable.get(b"key2", 3) == SSTableEntry(
        key=b"key2", seq_num=1, value=b"val2"
    )
    assert sstable.get(b"key3", 3) == SSTableEntry(
        key=b"key3", seq_num=2, value=TOMBSTONE
    )
    assert sstable.get(b"key4", 3) == None


@pytest.mark.skip("Disk SSTable not implemented")
def test_sstable_writer_to_disk(memtable: MemTable):
    sstable_writer = SSTableWriter(in_memory=False, dir=Path("/tmp/data"))
    memtable.insert(key=b"key1", seq_num=1, value=b"val1")

    sstable = sstable_writer.write(memtable, 1)[0]

    assert isinstance(sstable, SSTable)
    assert not isinstance(sstable, InMemorySSTable)
    assert len(sstable.index_keys) == 1
    assert sstable.file.tell() == 0
    assert len(sstable.file.getvalue()) > 0


def test_sstable_get(sstable_writer: SSTableWriter, memtable: MemTable):
    memtable.insert(key=b"apple", seq_num=1, value=b"red")
    memtable.insert(key=b"banana", seq_num=2, value=b"yellow")
    memtable.insert(key=b"cherry", seq_num=3, value=TOMBSTONE)

    sstable = sstable_writer.write(memtable, 1)[0]

    assert sstable.get(b"apple", 3) == SSTableEntry(
        key=b"apple", seq_num=1, value=b"red"
    )
    assert sstable.get(b"banana", 3) == SSTableEntry(
        key=b"banana", seq_num=2, value=b"yellow"
    )

    assert sstable.get(b"zebra", 3) is None
    assert sstable.get(b"cherry", 3) == SSTableEntry(
        key=b"cherry", seq_num=3, value=TOMBSTONE
    )


def test_sstable_iter(sstable_writer: SSTableWriter, memtable: MemTable):
    memtable.insert(key=b"apple", seq_num=1, value=b"red")
    memtable.insert(key=b"banana", seq_num=2, value=b"yellow")
    memtable.insert(key=b"cherry", seq_num=3, value=TOMBSTONE)
    memtable.insert(key=b"lime", seq_num=4, value=b"green")

    sstable = sstable_writer.write(memtable, 1)[0]

    ssentries = [ssentry for ssentry in sstable]
    assert extract_from_internal_key(ssentries[0][0]) == (b"apple", 1, 1)
    assert extract_from_internal_key(ssentries[1][0]) == (b"banana", 2, 1)
    assert extract_from_internal_key(ssentries[2][0]) == (b"cherry", 3, 0)
    assert extract_from_internal_key(ssentries[3][0]) == (b"lime", 4, 1)


def test_sstable_scan_fixed_range(sstable_writer: SSTableWriter, memtable: MemTable):
    memtable.insert(b"key1", 0, b"val1")
    memtable.insert(b"key2", 1, b"val2")
    memtable.insert(b"key3", 2, b"val3")

    sstable = sstable_writer.write(memtable, 1)[0]

    # Test inclusive
    result = [item for item in sstable.scan(b"key", b"key4", 2)]
    assert result == [
        SSTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        SSTableEntry(key=b"key2", seq_num=1, value=b"val2"),
        SSTableEntry(key=b"key3", seq_num=2, value=b"val3"),
    ]

    # Test excluding start_key
    result = [item for item in sstable.scan(b"key2", b"key4", 2)]
    assert result == [
        SSTableEntry(key=b"key2", seq_num=1, value=b"val2"),
        SSTableEntry(key=b"key3", seq_num=2, value=b"val3"),
    ]

    # Test excluding end_key
    result = [item for item in sstable.scan(b"key1", b"key3", 2)]
    assert result == [
        SSTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        SSTableEntry(key=b"key2", seq_num=1, value=b"val2"),
    ]

    # Test excluding both
    result = [item for item in sstable.scan(b"key2", b"key3", 2)]
    assert result == [
        SSTableEntry(key=b"key2", seq_num=1, value=b"val2"),
    ]


def test_sstable_scan_unbounded_range(
    sstable_writer: SSTableWriter, memtable: MemTable
):
    memtable.insert(b"key1", 0, b"val1")
    memtable.insert(b"key2", 1, b"val2")
    memtable.insert(b"key3", 2, b"val3")

    sstable = sstable_writer.write(memtable, 1)[0]

    # Test unbounded end_key
    result = [item for item in sstable.scan(b"key1", None, 2)]
    assert result == [
        SSTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        SSTableEntry(key=b"key2", seq_num=1, value=b"val2"),
        SSTableEntry(key=b"key3", seq_num=2, value=b"val3"),
    ]

    # Test unbounded start_key
    result = [item for item in sstable.scan(None, b"key4", 2)]
    assert result == [
        SSTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        SSTableEntry(key=b"key2", seq_num=1, value=b"val2"),
        SSTableEntry(key=b"key3", seq_num=2, value=b"val3"),
    ]

    # Test unbounded both
    result = [item for item in sstable.scan(None, None, 2)]
    assert result == [
        SSTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        SSTableEntry(key=b"key2", seq_num=1, value=b"val2"),
        SSTableEntry(key=b"key3", seq_num=2, value=b"val3"),
    ]


def test_sstable_scan_max_seq(sstable_writer: SSTableWriter, memtable: MemTable):
    memtable.insert(b"key1", 0, b"val1")
    memtable.insert(b"key2", 1, b"val2")
    memtable.insert(b"key3", 2, b"val3")

    sstable = sstable_writer.write(memtable, 1)[0]

    # Test fixed range
    result = [item for item in sstable.scan(b"key1", b"key4", 1)]
    assert result == [
        SSTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        SSTableEntry(key=b"key2", seq_num=1, value=b"val2"),
    ]

    # Test unbounded
    result = [item for item in sstable.scan(None, None, 1)]
    assert result == [
        SSTableEntry(key=b"key1", seq_num=0, value=b"val1"),
        SSTableEntry(key=b"key2", seq_num=1, value=b"val2"),
    ]


def test_sstable_scan_all_occurence_of_a_key(
    sstable_writer: SSTableWriter, memtable: MemTable
):
    memtable.insert(b"key1", 0, b"val1")
    memtable.insert(b"key1", 1, TOMBSTONE)

    sstable = sstable_writer.write(memtable, 1)[0]

    result = [item for item in sstable.scan(None, None, 1)]

    # Order guarantee is descending per seq_num
    assert result == [
        SSTableEntry(key=b"key1", seq_num=1, value=TOMBSTONE),
        SSTableEntry(key=b"key1", seq_num=0, value=b"val1"),
    ]
