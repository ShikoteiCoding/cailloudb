import io
from pathlib import Path

import pytest

from cailloudb.custom_types import SSTableEntry
from cailloudb.lsm.memtable import MemTable
from cailloudb.lsm.sstable import (
    InMemorySSTable,
    SSTable,
)
from cailloudb.lsm.table_builder import TableBuilder, write_iterator_to_sstables
from cailloudb.lsm.utils import build_internal_key
from cailloudb.settings import (
    SSTABLE_MAGIC_NUMBER,
    STRUCT_INTERNAL_KEY_LEN,
    STRUCT_SSTABLE_FOOTER,
    STRUCT_SSTABLE_INDEX_OFFSET_LEN,
    TOMBSTONE,
    Settings,
)

__all__ = [
    "test_table_builder_add",
    "test_table_builder_flushes_block",
    "test_table_builder_finish_state_transition_and_file_layout",
    "test_table_builder_finish_state_transition_and_file_layout_durable",
    "test_table_builder_durable_and_inmemory_produce_identical_bytes",
    "test_write_iterator_to_sstables_inmemory_write_returns",
    "test_write_iterator_to_sstables_durable_write_returns",
    "test_write_iterator_to_sstables_inmemory_write_sstable",
    "test_write_iterator_to_sstables_durable_write_sstable",
    "test_write_iterator_to_sstables_splits_when_builder_is_full",
    "test_write_iterator_to_sstables_durable_splits_when_builder_is_full",
]

settings = Settings()
SSTABLE_MAX_FILE_SIZE = settings.sstable_max_file_size
SSTABLE_BLOCK_DEFAULT_SIZE = settings.sstable_block_default_size


@pytest.fixture
def memtable() -> MemTable:
    return MemTable(Settings().memtable_max_bytes_size)


@pytest.fixture
def table_builder_not_durable() -> TableBuilder:
    return TableBuilder(
        file_id=0,
        sstable_block_default_size=SSTABLE_BLOCK_DEFAULT_SIZE,
        durable_sstable=False,
    )


def _fixed_file_id_generator(file_id: int):
    return lambda: file_id


def _sequential_file_id_generator(start: int):
    next_file_id = start

    def _next() -> int:
        nonlocal next_file_id
        current = next_file_id
        next_file_id += 1
        return current

    return _next


def test_table_builder_add(table_builder_not_durable: TableBuilder):
    table_builder_not_durable.add(b"internal_key_1", b"val1")
    table_builder_not_durable.add(b"internal_key_2", b"val2")

    assert table_builder_not_durable.smallest_key == b"internal_key_1"
    assert table_builder_not_durable.largest_key == b"internal_key_2"


def test_table_builder_flushes_block():
    table_builder = TableBuilder(
        file_id=0,
        sstable_block_default_size=30,  # Force small block size
        durable_sstable=False,
        sstable_dir=None,
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
    table_builder = TableBuilder(
        file_id=1,
        sstable_block_default_size=SSTABLE_BLOCK_DEFAULT_SIZE,
        durable_sstable=False,
        sstable_dir=None,
    )
    key_1 = build_internal_key(b"apple", 1, False)
    key_2 = build_internal_key(b"banana", 2, False)

    table_builder.add(key_1, b"red")
    table_builder.add(key_2, b"yellow")

    # Before finish, records are still buffered and no index/data block is finalized.
    assert table_builder.block_builder.is_empty() is False
    assert table_builder.index_entries == []
    assert table_builder.current_offset == 0
    assert table_builder.writer.tell() >= 0

    sstable, meta = table_builder.finish()

    # finish() flushes active block, writes index + footer, and rewinds file.
    assert table_builder.block_builder.is_empty() is True
    assert len(table_builder.index_entries) == 1
    assert table_builder.current_offset == table_builder.index_entries[0][2]
    assert sstable.file.tell() == 0

    assert sstable.index_keys == [key_2]
    assert sstable.index_meta == [
        (table_builder.index_entries[0][1], table_builder.index_entries[0][2])
    ]

    assert meta["smallest_key"] == key_1
    assert meta["largest_key"] == key_2
    assert meta["file_size"] == len(sstable.file.read())

    sstable.file.seek(0)
    file_bytes = sstable.file.read()
    footer = file_bytes[-STRUCT_SSTABLE_FOOTER.size :]
    index_offset, index_size, magic = STRUCT_SSTABLE_FOOTER.unpack(footer)

    expected_index_size = (
        STRUCT_INTERNAL_KEY_LEN.size + len(key_2) + STRUCT_SSTABLE_INDEX_OFFSET_LEN.size
    )
    assert index_offset == table_builder.current_offset
    assert index_size == expected_index_size
    assert magic == SSTABLE_MAGIC_NUMBER


def test_table_builder_finish_state_transition_and_file_layout_durable(tmp_path: Path):
    durable_settings = Settings()

    table_builder = TableBuilder(
        file_id=1,
        sstable_block_default_size=SSTABLE_BLOCK_DEFAULT_SIZE,
        durable_sstable=True,
        sstable_dir=tmp_path,
    )
    key_1 = build_internal_key(b"apple", 1, False)
    key_2 = build_internal_key(b"banana", 2, False)

    table_builder.add(key_1, b"red")
    table_builder.add(key_2, b"yellow")

    # Before finish, records are still buffered and no index/data block is finalized.
    assert table_builder.block_builder.is_empty() is False
    assert table_builder.index_entries == []
    assert table_builder.current_offset == 0
    assert table_builder.writer.tell() >= 0

    sstable, meta = table_builder.finish()

    # finish() flushes active block, writes index + footer, and rewinds file.
    assert table_builder.block_builder.is_empty() is True
    assert len(table_builder.index_entries) == 1
    assert table_builder.current_offset == table_builder.index_entries[0][2]
    assert sstable.file.tell() == 0

    assert sstable.index_keys == [key_2]
    assert sstable.index_meta == [
        (table_builder.index_entries[0][1], table_builder.index_entries[0][2])
    ]

    assert meta["smallest_key"] == key_1
    assert meta["largest_key"] == key_2
    assert meta["file_size"] == len(sstable.file.read())

    sstable.file.seek(0)
    file_bytes = sstable.file.read()
    footer = file_bytes[-STRUCT_SSTABLE_FOOTER.size :]
    index_offset, index_size, magic = STRUCT_SSTABLE_FOOTER.unpack(footer)

    expected_index_size = (
        STRUCT_INTERNAL_KEY_LEN.size + len(key_2) + STRUCT_SSTABLE_INDEX_OFFSET_LEN.size
    )
    assert index_offset == table_builder.current_offset
    assert index_size == expected_index_size
    assert magic == SSTABLE_MAGIC_NUMBER


def test_table_builder_durable_and_inmemory_produce_identical_bytes(tmp_path: Path):
    durable_settings = Settings()

    records = [
        (build_internal_key(b"apple", 3, False), b"red"),
        (build_internal_key(b"banana", 2, False), b"yellow"),
        (build_internal_key(b"carrot", 1, True), TOMBSTONE),
    ]

    inmemory_builder = TableBuilder(
        file_id=99,
        sstable_block_default_size=SSTABLE_BLOCK_DEFAULT_SIZE,
        durable_sstable=False,
    )
    durable_builder = TableBuilder(
        file_id=99,
        sstable_block_default_size=SSTABLE_BLOCK_DEFAULT_SIZE,
        durable_sstable=True,
        sstable_dir=tmp_path,
    )

    for internal_key, value in records:
        inmemory_builder.add(internal_key, value)
        durable_builder.add(internal_key, value)

    inmemory_sstable, inmemory_meta = inmemory_builder.finish()
    durable_sstable, durable_meta = durable_builder.finish()

    inmemory_bytes = inmemory_sstable.file.read()
    durable_bytes = durable_sstable.file.read()

    assert inmemory_bytes == durable_bytes
    assert inmemory_meta == durable_meta


@pytest.mark.parametrize(
    "in_memory,expected_cls",
    [
        (True, InMemorySSTable),
        (False, SSTable),
    ],
)
def test_write_iterator_to_sstables_inmemory_write_returns(
    in_memory: bool, expected_cls: type[SSTable]
):
    memtable = MemTable(Settings().sstable_max_file_size)
    memtable.insert(key=b"carrot", seq_num=1, value=b"orange")
    memtable.insert(key=b"banana", seq_num=2, value=TOMBSTONE)
    memtable.insert(key=b"apple", seq_num=3, value=b"red")

    sstable, _ = write_iterator_to_sstables(
        memtable,
        _fixed_file_id_generator(1),
        SSTABLE_MAX_FILE_SIZE,
        SSTABLE_BLOCK_DEFAULT_SIZE,
        False,
    )[0]

    assert sstable.file.tell() == 0
    assert sstable.file_id == 1


def test_write_iterator_to_sstables_durable_write_returns(
    tmp_path: Path,
):
    durable_settings = Settings()

    memtable = MemTable(Settings().sstable_max_file_size)
    memtable.insert(key=b"carrot", seq_num=1, value=b"orange")
    memtable.insert(key=b"banana", seq_num=2, value=TOMBSTONE)
    memtable.insert(key=b"apple", seq_num=3, value=b"red")

    sstable, _ = write_iterator_to_sstables(
        memtable,
        _fixed_file_id_generator(1),
        SSTABLE_MAX_FILE_SIZE,
        SSTABLE_BLOCK_DEFAULT_SIZE,
        True,
        tmp_path,
    )[0]

    assert sstable.__class__.__name__ == "DurableSSTable"
    assert sstable.file.tell() == 0
    assert sstable.file_id == 1


def test_write_iterator_to_sstables_inmemory_write_sstable(memtable: MemTable):
    memtable.insert(key=b"key1", seq_num=0, value=b"val1")
    memtable.insert(key=b"key2", seq_num=1, value=b"val2")
    memtable.insert(key=b"key3", seq_num=2, value=TOMBSTONE)

    sstable, _ = write_iterator_to_sstables(
        memtable,
        _fixed_file_id_generator(1),
        SSTABLE_MAX_FILE_SIZE,
        SSTABLE_BLOCK_DEFAULT_SIZE,
        False,
    )[0]

    # Check sparse index
    assert len(sstable.index_keys) == 1  # 1 block ~4KB holds the 2 records

    # Check files
    assert sstable.file.tell() == 0
    assert len(sstable.file.read()) > 0

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


def test_write_iterator_to_sstables_durable_write_sstable(
    memtable: MemTable, tmp_path: Path
):
    durable_settings = Settings()

    memtable.insert(key=b"key1", seq_num=1, value=b"val1")
    memtable.insert(key=b"key2", seq_num=2, value=b"val2")
    memtable.insert(key=b"key3", seq_num=3, value=TOMBSTONE)

    sstable, _ = write_iterator_to_sstables(
        memtable,
        _fixed_file_id_generator(1),
        SSTABLE_MAX_FILE_SIZE,
        SSTABLE_BLOCK_DEFAULT_SIZE,
        True,
        tmp_path,
    )[0]
    assert len(sstable.index_keys) == 1
    assert sstable.file.tell() == 0
    assert len(sstable.file.read()) > 0

    ssentries = [ssentry for ssentry in sstable]
    assert len(ssentries) == 3
    assert sstable.get(b"key1", 3) == SSTableEntry(
        key=b"key1", seq_num=1, value=b"val1"
    )
    assert sstable.get(b"key2", 3) == SSTableEntry(
        key=b"key2", seq_num=2, value=b"val2"
    )
    assert sstable.get(b"key3", 3) == SSTableEntry(
        key=b"key3", seq_num=3, value=TOMBSTONE
    )
    assert sstable.get(b"key4", 3) == None


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
        file_id_generator=_sequential_file_id_generator(10),
        max_file_size=1,  # Force small file size
        target_block_size=SSTABLE_BLOCK_DEFAULT_SIZE,
        durable_sstable=False,
    )

    assert len(results) == 3

    for idx, (sstable, meta) in enumerate(results):
        expected_file_id = 10 + idx
        expected_key = bytes([ord("a") + idx])
        expected_value = f"v{idx + 1}".encode()

        assert sstable.file_id == expected_file_id
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


def test_write_iterator_to_sstables_durable_splits_when_builder_is_full(
    tmp_path: Path,
):
    records = iter(
        [
            (build_internal_key(b"a", 3, False), b"v1"),
            (build_internal_key(b"b", 2, False), b"v2"),
            (build_internal_key(b"c", 1, False), b"v3"),
        ]
    )

    results = write_iterator_to_sstables(
        records,
        file_id_generator=_sequential_file_id_generator(10),
        max_file_size=1,  # Force small file size
        target_block_size=SSTABLE_BLOCK_DEFAULT_SIZE,
        durable_sstable=True,
        sstable_dir=tmp_path,
    )

    assert len(results) == 3

    for idx, (sstable, meta) in enumerate(results):
        expected_file_id = 10 + idx
        expected_key = bytes([ord("a") + idx])
        expected_value = f"v{idx + 1}".encode()

        assert sstable.__class__.__name__ == "DurableSSTable"
        assert sstable.file_id == expected_file_id
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
