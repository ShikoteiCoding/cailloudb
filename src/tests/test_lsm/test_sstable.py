from pathlib import Path

import pytest

from cailloudb.constants import TOMBSTONE
from cailloudb.custom_types import MemTableEntry, SSTableEntry
from cailloudb.lsm.memtable import MemTable
from cailloudb.lsm.sstable import (
    InMemorySSTable,
    SSTable,
    SSTableWriter,
    encode_memtable_entry,
)

__all__ = [
    "test_sstable_writer_encode_memtable_entry_standard",
    "test_sstable_writer_encode_memtable_entry_tombstone",
    "test_sstable_writer_write_sstable",
    "test_sstable_writer_write_returns_and_internals",
    "test_sstable_writer_to_disk",
    "test_sstable_get",
    "test_sstable_iter",
    "test_sstable_scan_fixed_range",
    "test_sstable_scan_unbounded_range",
    "test_sstable_scan_max_seq",
    "test_sstable_scan_tombstone",
]


@pytest.fixture
def sstable_writer() -> SSTableWriter:
    return SSTableWriter(in_memory=True, dir=Path("/tmp/data"))


@pytest.fixture
def memtable() -> MemTable:
    return MemTable()


def test_sstable_writer_encode_memtable_entry_standard():
    entry = MemTableEntry(**{"key": b"key1", "seq_num": 1, "value": b"val1"})

    buf, size = encode_memtable_entry(entry)

    assert isinstance(buf, bytearray)
    assert size == len(buf)
    assert size > 0


def test_sstable_writer_encode_memtable_entry_tombstone():
    entry = MemTableEntry(**{"key": b"key1", "seq_num": 1, "value": TOMBSTONE})

    buf, size = encode_memtable_entry(entry)

    assert isinstance(buf, bytearray)
    assert size == len(buf)
    assert size > 0


def test_sstable_writer_write_sstable(
    sstable_writer: SSTableWriter, memtable: MemTable
):
    memtable.insert(key=b"key1", seq_num=1, value=b"val1")
    memtable.insert(key=b"key2", seq_num=2, value=b"val2")

    result = sstable_writer.write(memtable)

    assert isinstance(result, InMemorySSTable)
    assert len(result.offsets) == 2
    assert result.file.tell() == 0
    assert len(result.file.getvalue()) > 0


@pytest.mark.parametrize(
    "in_memory,expected_cls",
    [
        (True, InMemorySSTable),
        (False, SSTable),
    ],
)
def test_sstable_writer_write_returns_and_internals(
    in_memory: bool, expected_cls: type[SSTable]
):
    memtable = MemTable()
    memtable.insert(key=b"carrot", seq_num=1, value=b"orange")
    memtable.insert(key=b"banana", seq_num=2, value=TOMBSTONE)
    memtable.insert(key=b"apple", seq_num=3, value=b"red")

    expected_offsets: list[int] = []
    expected_chunks: list[bytes] = []
    running_offset = 0

    for entry in memtable:
        expected_offsets.append(running_offset)
        encoded, entry_size = encode_memtable_entry(entry)
        expected_chunks.append(bytes(encoded))
        running_offset += entry_size

    writer = SSTableWriter(in_memory=in_memory, dir=Path("/tmp/data"))
    result = writer.write(memtable)

    assert isinstance(result, expected_cls)
    assert result.path == Path()
    assert result.file.tell() == 0
    assert result.offsets == expected_offsets
    assert result.file.getvalue() == b"".join(expected_chunks)


@pytest.mark.skip("Disk SSTable not implemented")
def test_sstable_writer_to_disk(memtable: MemTable):
    sstable_writer = SSTableWriter(in_memory=False, dir=Path("/tmp/data"))
    memtable.insert(key=b"key1", seq_num=1, value=b"val1")

    result = sstable_writer.write(memtable)

    assert isinstance(result, SSTable)
    assert not isinstance(result, InMemorySSTable)
    assert len(result.offsets) == 1
    assert result.file.tell() == 0
    assert len(result.file.getvalue()) > 0


def test_sstable_get(sstable_writer: SSTableWriter, memtable: MemTable):
    memtable.insert(key=b"apple", seq_num=1, value=b"red")
    memtable.insert(key=b"banana", seq_num=2, value=b"yellow")
    memtable.insert(key=b"cherry", seq_num=3, value=TOMBSTONE)

    sstable = sstable_writer.write(memtable)

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

    sstable = sstable_writer.write(memtable)

    ssentries = [ssentry for ssentry in sstable]
    assert ssentries[0] == SSTableEntry(key=b"apple", seq_num=1, value=b"red")
    assert ssentries[1] == SSTableEntry(key=b"banana", seq_num=2, value=b"yellow")
    assert ssentries[2] == SSTableEntry(key=b"cherry", seq_num=3, value=TOMBSTONE)
    assert ssentries[3] == SSTableEntry(key=b"lime", seq_num=4, value=b"green")


def test_sstable_scan_fixed_range(sstable_writer: SSTableWriter, memtable: MemTable):
    memtable.insert(b"key1", 0, b"val1")
    memtable.insert(b"key2", 1, b"val2")
    memtable.insert(b"key3", 2, b"val3")

    sstable = sstable_writer.write(memtable)

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

    sstable = sstable_writer.write(memtable)

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

    sstable = sstable_writer.write(memtable)

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


def test_sstable_scan_tombstone(sstable_writer: SSTableWriter, memtable: MemTable):
    memtable.insert(b"key1", 0, b"val1")
    memtable.insert(b"key1", 1, TOMBSTONE)

    sstable = sstable_writer.write(memtable)

    result = [item for item in sstable.scan(None, None, 1)]

    # Order guarantee is descending per seq_num
    assert result == [
        SSTableEntry(key=b"key1", seq_num=1, value=TOMBSTONE),
        SSTableEntry(key=b"key1", seq_num=0, value=b"val1"),
    ]
