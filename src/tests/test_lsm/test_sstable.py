import io
from pathlib import Path

import pytest

from cailloudb.custom_types import SSTableEntry
from cailloudb.lsm.memtable import MemTable
from cailloudb.lsm.sstable import (
    InMemorySSTable,
    SSTable,
)
from cailloudb.lsm.table_builder import write_iterator_to_sstables
from cailloudb.lsm.utils import extract_from_internal_key
from cailloudb.settings import (
    SSTABLE_MAGIC_NUMBER,
    STRUCT_SSTABLE_FOOTER,
    TOMBSTONE,
    Settings,
)

__all__ = [
    "test_sstable_get",
    "test_sstable_get_missing_key_in_range",
    "test_sstable_iter",
    "test_sstable_scan_fixed_range",
    "test_sstable_scan_unbounded_range",
    "test_sstable_scan_max_seq",
    "test_sstable_scan_all_occurence_of_a_key",
]

settings = Settings()
SSTABLE_MAX_FILE_SIZE = settings.sstable_max_file_size
SSTABLE_BLOCK_DEFAULT_SIZE = settings.sstable_block_default_size


@pytest.fixture
def memtable() -> MemTable:
    return MemTable(Settings().memtable_max_bytes_size)


def _fixed_file_id_generator(file_id: int):
    return lambda: file_id


def test_sstable_get(memtable: MemTable):
    memtable.insert(key=b"apple", seq_num=1, value=b"red")
    memtable.insert(key=b"banana", seq_num=2, value=b"yellow")
    memtable.insert(key=b"cherry", seq_num=3, value=TOMBSTONE)

    sstable, _ = write_iterator_to_sstables(
        memtable,
        _fixed_file_id_generator(1),
        SSTABLE_MAX_FILE_SIZE,
        SSTABLE_BLOCK_DEFAULT_SIZE,
    )[0]

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


def test_sstable_get_missing_key_in_range(memtable: MemTable):
    memtable.insert(key=b"apple", seq_num=1, value=b"red")
    memtable.insert(key=b"cherry", seq_num=2, value=b"dark-red")

    sstable, _file_metadata = write_iterator_to_sstables(
        memtable,
        _fixed_file_id_generator(1),
        SSTABLE_MAX_FILE_SIZE,
        SSTABLE_BLOCK_DEFAULT_SIZE,
    )[0]

    assert sstable.get(b"banana", 2) is None


def test_sstable_iter(memtable: MemTable):
    memtable.insert(key=b"apple", seq_num=1, value=b"red")
    memtable.insert(key=b"banana", seq_num=2, value=b"yellow")
    memtable.insert(key=b"cherry", seq_num=3, value=TOMBSTONE)
    memtable.insert(key=b"lime", seq_num=4, value=b"green")

    sstable, _file_metadata = write_iterator_to_sstables(
        memtable,
        _fixed_file_id_generator(1),
        SSTABLE_MAX_FILE_SIZE,
        SSTABLE_BLOCK_DEFAULT_SIZE,
    )[0]

    ssentries = [ssentry for ssentry in sstable]
    assert extract_from_internal_key(ssentries[0][0]) == (b"apple", 1, 1)
    assert extract_from_internal_key(ssentries[1][0]) == (b"banana", 2, 1)
    assert extract_from_internal_key(ssentries[2][0]) == (b"cherry", 3, 0)
    assert extract_from_internal_key(ssentries[3][0]) == (b"lime", 4, 1)


def test_sstable_scan_fixed_range(memtable: MemTable):
    memtable.insert(b"key1", 0, b"val1")
    memtable.insert(b"key2", 1, b"val2")
    memtable.insert(b"key3", 2, b"val3")

    sstable, _file_metadata = write_iterator_to_sstables(
        memtable,
        _fixed_file_id_generator(1),
        SSTABLE_MAX_FILE_SIZE,
        SSTABLE_BLOCK_DEFAULT_SIZE,
    )[0]

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

    # Test out of range before
    result = [item for item in sstable.scan(b"a", b"b", 10)]
    assert result == []

    # Test out of range after
    result = [item for item in sstable.scan(b"key4", b"key5", 10)]
    assert result == []


def test_sstable_scan_unbounded_range(memtable: MemTable):
    memtable.insert(b"key1", 0, b"val1")
    memtable.insert(b"key2", 1, b"val2")
    memtable.insert(b"key3", 2, b"val3")

    sstable, _file_metadata = write_iterator_to_sstables(
        memtable,
        _fixed_file_id_generator(1),
        SSTABLE_MAX_FILE_SIZE,
        SSTABLE_BLOCK_DEFAULT_SIZE,
    )[0]

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


def test_sstable_scan_max_seq(memtable: MemTable):
    memtable.insert(b"key1", 0, b"val1")
    memtable.insert(b"key2", 1, b"val2")
    memtable.insert(b"key3", 2, b"val3")

    sstable, _file_metadata = write_iterator_to_sstables(
        memtable,
        _fixed_file_id_generator(1),
        SSTABLE_MAX_FILE_SIZE,
        SSTABLE_BLOCK_DEFAULT_SIZE,
    )[0]

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


def test_sstable_scan_all_occurence_of_a_key(memtable: MemTable):
    memtable.insert(b"key1", 0, b"val1")
    memtable.insert(b"key1", 1, TOMBSTONE)

    sstable, _file_metadata = write_iterator_to_sstables(
        memtable,
        _fixed_file_id_generator(1),
        SSTABLE_MAX_FILE_SIZE,
        SSTABLE_BLOCK_DEFAULT_SIZE,
    )[0]

    result = [item for item in sstable.scan(None, None, 1)]

    # Order guarantee is descending per seq_num
    assert result == [
        SSTableEntry(key=b"key1", seq_num=1, value=TOMBSTONE),
        SSTableEntry(key=b"key1", seq_num=0, value=b"val1"),
    ]


def test_sstable_SSTABLE_MAGIC_NUMBER_fence(memtable: MemTable):
    memtable.insert(b"key1", 0, b"val1")

    valid_sstable, _file_metadata = write_iterator_to_sstables(
        memtable,
        _fixed_file_id_generator(1),
        SSTABLE_MAX_FILE_SIZE,
        SSTABLE_BLOCK_DEFAULT_SIZE,
    )[0]
    valid_bytes = valid_sstable.file.getvalue()

    index_offset, index_size, magic = STRUCT_SSTABLE_FOOTER.unpack(
        valid_bytes[-STRUCT_SSTABLE_FOOTER.size :]
    )
    assert magic == SSTABLE_MAGIC_NUMBER

    reloaded_sstable = SSTable(1, io.BytesIO(valid_bytes), Path("valid_magic.sst"))
    assert reloaded_sstable.index_keys == valid_sstable.index_keys
    assert reloaded_sstable.index_meta == valid_sstable.index_meta

    invalid_magic = (SSTABLE_MAGIC_NUMBER + 1) & 0xFFFFFFFF
    invalid_footer = STRUCT_SSTABLE_FOOTER.pack(index_offset, index_size, invalid_magic)
    invalid_bytes = valid_bytes[: -STRUCT_SSTABLE_FOOTER.size] + invalid_footer

    with pytest.raises(ValueError, match="Invalid magic number"):
        SSTable(1, io.BytesIO(invalid_bytes), Path("invalid_magic.sst"))
