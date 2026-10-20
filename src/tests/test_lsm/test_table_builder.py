import io
from pathlib import Path

import pytest

from cailloudb.constants import (
    INTERNAL_KEY_LEN_STRUCT,
    SSTABLE_FOOTER_STRUCT,
    SSTABLE_INDEX_BLOCK_OFFSET_AND_LEN_STRUCT,
    SSTABLE_MAGIC_NUMBER,
    SSTABLE_MAX_FILE_SIZE,
    TOMBSTONE,
)
from cailloudb.custom_types import SSTableEntry
from cailloudb.lsm.memtable import MemTable
from cailloudb.lsm.sstable import (
    InMemorySSTable,
    SSTable,
)
from cailloudb.lsm.table_builder import TableBuilder, write_iterator_to_sstables
from cailloudb.lsm.utils import build_internal_key

__all__ = [
    "test_write_iterator_to_sstables_write_returns",
    "test_write_iterator_to_sstables_write_sstable",
    "test_write_iterator_to_sstables_to_disk",
]


@pytest.fixture
def memtable() -> MemTable:
    return MemTable()


@pytest.fixture
def table_builder() -> TableBuilder:
    return TableBuilder(0, True)


def test_table_builder_add(table_builder: TableBuilder):
    table_builder.add(b"internal_key_1", b"val1")
    table_builder.add(b"internal_key_2", b"val2")

    assert table_builder.smallest_key == b"internal_key_1"
    assert table_builder.largest_key == b"internal_key_2"


def test_table_builder_flushes_block():
    table_builder = TableBuilder(
        0,
        True,
        None,
        30,  # Force small block size
    )
    table_builder.add(b"internal_key_1", b"val1")
    assert len(table_builder.index_entries) == 0

    table_builder.add(b"internal_key_2", b"val2")
    assert len(table_builder.index_entries) == 0

    table_builder.add(b"internal_key_3", b"val3")
    assert len(table_builder.index_entries) == 1

    # key metadata is retained accross flushes
    assert table_builder.smallest_key == b"internal_key_1"
    assert table_builder.largest_key == b"internal_key_3"


def test_table_builder_finish_state_transition_and_file_layout():
    table_builder = TableBuilder(file_id=1, in_memory=True)
    key_1 = build_internal_key(b"apple", 1, False)
    key_2 = build_internal_key(b"banana", 2, False)

    table_builder.add(key_1, b"red")
    table_builder.add(key_2, b"yellow")

    # Before finish, records are still buffered and no index/data block is finalized.
    assert table_builder.block_builder.is_empty() is False
    assert table_builder.index_entries == []
    assert table_builder.current_offset == 0
    assert table_builder.out.tell() == 0

    sstable, meta = table_builder.finish()

    # finish() flushes active block, writes index + footer, and rewinds file.
    assert table_builder.block_builder.is_empty() is True
    assert len(table_builder.index_entries) == 1
    assert table_builder.current_offset == table_builder.index_entries[0][2]
    assert sstable.file.tell() == 0

    assert sstable.path == Path("000001.sst")
    assert sstable.index_keys == [key_2]
    assert sstable.index_meta == [
        (table_builder.index_entries[0][1], table_builder.index_entries[0][2])
    ]

    assert meta["smallest_key"] == key_1
    assert meta["largest_key"] == key_2
    assert meta["file_size"] == len(sstable.file.getvalue())

    file_bytes = sstable.file.getvalue()
    footer = file_bytes[-SSTABLE_FOOTER_STRUCT.size :]
    index_offset, index_size, magic = SSTABLE_FOOTER_STRUCT.unpack(footer)

    expected_index_size = (
        INTERNAL_KEY_LEN_STRUCT.size
        + len(key_2)
        + SSTABLE_INDEX_BLOCK_OFFSET_AND_LEN_STRUCT.size
    )
    assert index_offset == table_builder.current_offset
    assert index_size == expected_index_size
    assert magic == SSTABLE_MAGIC_NUMBER


@pytest.mark.parametrize(
    "in_memory,expected_cls",
    [
        (True, InMemorySSTable),
        (False, SSTable),
    ],
)
def test_write_iterator_to_sstables_write_returns(
    in_memory: bool, expected_cls: type[SSTable]
):
    memtable = MemTable()
    memtable.insert(key=b"carrot", seq_num=1, value=b"orange")
    memtable.insert(key=b"banana", seq_num=2, value=TOMBSTONE)
    memtable.insert(key=b"apple", seq_num=3, value=b"red")

    sstable, file_metadata = write_iterator_to_sstables(
        memtable, 1, SSTABLE_MAX_FILE_SIZE
    )[0]

    assert sstable.path == Path("000001.sst")
    assert sstable.file.tell() == 0
    assert sstable.file_id == 1


def test_write_iterator_to_sstables_write_sstable(memtable: MemTable):
    memtable.insert(key=b"key1", seq_num=0, value=b"val1")
    memtable.insert(key=b"key2", seq_num=1, value=b"val2")
    memtable.insert(key=b"key3", seq_num=2, value=TOMBSTONE)

    sstable, file_metadata = write_iterator_to_sstables(
        memtable, 1, SSTABLE_MAX_FILE_SIZE
    )[0]

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
def test_write_iterator_to_sstables_to_disk(memtable: MemTable):
    memtable.insert(key=b"key1", seq_num=1, value=b"val1")

    sstable, file_metadata = write_iterator_to_sstables(
        memtable, 1, SSTABLE_MAX_FILE_SIZE
    )[0]

    assert isinstance(sstable, SSTable)
    assert not isinstance(sstable, InMemorySSTable)
    assert len(sstable.index_keys) == 1
    assert sstable.file.tell() == 0
    assert len(sstable.file.getvalue()) > 0


def test_write_iterator_to_sstables_splits_when_builder_is_full():
    records = iter(
        [
            (build_internal_key(b"a", 3, False), b"v1"),
            (build_internal_key(b"b", 2, False), b"v2"),
            (build_internal_key(b"c", 1, False), b"v3"),
        ]
    )

    results = write_iterator_to_sstables(
        records,
        start_file_id=10,
        max_file_size=1,  # Force small file size
        in_memory=True,
    )

    assert len(results) == 3

    for idx, (sstable, meta) in enumerate(results):
        expected_file_id = 10 + idx
        expected_key = bytes([ord("a") + idx])
        expected_value = f"v{idx + 1}".encode()

        assert sstable.file_id == expected_file_id
        assert sstable.path == Path(f"{expected_file_id:06d}.sst")
        assert sstable.file.tell() == 0
        assert len(sstable.index_keys) == 1

        assert sstable.get(expected_key, 3) == SSTableEntry(
            key=expected_key,
            seq_num=3 - idx,
            value=expected_value,
        )

        assert meta["smallest_key"] == sstable.index_keys[0]
        assert meta["largest_key"] == sstable.index_keys[0]
        assert meta["file_size"] > 0
