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


def test_sstable_writer_write_sstable():
    writer = SSTableWriter(in_memory=True)
    memtable = MemTable()
    memtable.put(key=b"key1", seq_num=1, value=b"val1")
    memtable.put(key=b"key2", seq_num=2, value=b"val2")

    result = writer.write(memtable)

    assert isinstance(result, InMemorySSTable)
    assert len(result.offsets) == 2
    assert result.file.tell() == 0
    assert len(result.file.getvalue()) > 0


@pytest.mark.skip("Disk SSTable not implemented")
def test_sstable_writer_to_disk():
    writer = SSTableWriter(in_memory=False, dir=Path("/tmp/data"))
    memtable = MemTable()
    memtable.put(key=b"key1", seq_num=1, value=b"val1")

    result = writer.write(memtable)

    assert isinstance(result, SSTable)
    assert not isinstance(result, InMemorySSTable)
    assert len(result.offsets) == 1
    assert result.file.tell() == 0
    assert len(result.file.getvalue()) > 0


def test_sstable_get():
    writer = SSTableWriter(in_memory=True)
    memtable = MemTable()

    memtable.put(key=b"apple", seq_num=1, value=b"red")
    memtable.put(key=b"banana", seq_num=2, value=b"yellow")
    memtable.put(key=b"cherry", seq_num=3, value=TOMBSTONE)

    sstable = writer.write(memtable)

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


def test_sstable_iter():
    writer = SSTableWriter(in_memory=True)
    memtable = MemTable()

    memtable.put(key=b"apple", seq_num=1, value=b"red")
    memtable.put(key=b"banana", seq_num=2, value=b"yellow")
    memtable.put(key=b"cherry", seq_num=3, value=TOMBSTONE)
    memtable.put(key=b"lime", seq_num=4, value=b"green")

    sstable = writer.write(memtable)

    ssentries = [ssentry for ssentry in sstable]
    assert ssentries[0] == SSTableEntry(key=b"apple", seq_num=1, value=b"red")
    assert ssentries[1] == SSTableEntry(key=b"banana", seq_num=2, value=b"yellow")
    assert ssentries[2] == SSTableEntry(key=b"cherry", seq_num=3, value=TOMBSTONE)
    assert ssentries[3] == SSTableEntry(key=b"lime", seq_num=4, value=b"green")
