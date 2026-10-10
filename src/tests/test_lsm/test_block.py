from cailloudb.lsm.block import BlockBuilder, BlockReader
from cailloudb.lsm.utils import build_internal_key

__all__ = ["test_block_encode_and_decode"]


def test_block_encode_and_decode():
    block_builder = BlockBuilder(1024)

    key, value = b"key1", b"val1"
    internal_key = build_internal_key(key, 0, False)
    block_builder.add(internal_key, value)
    block_data, last_internal_key = block_builder.finalize()

    assert BlockReader(block_data).get(key, 0) == (value, 0, False)
    assert last_internal_key == internal_key


def test_block_with_updated_values_keeps_ordering_guarantees():
    block_builder = BlockBuilder(1024)

    key1, value1 = b"key1", b"val1"
    internal_key1 = build_internal_key(key1, 0, False)

    key2, value2 = b"key1", b"val2"
    internal_key2 = build_internal_key(key2, 1, False)

    key3, value3 = b"key1", b""
    internal_key3 = build_internal_key(key3, 2, True)

    # Simulate SSTable insertion (key ascending, seq_num descending)
    block_builder.add(internal_key3, value3)
    block_builder.add(internal_key2, value2)
    block_builder.add(internal_key1, value1)

    block_data, last_internal_key = block_builder.finalize()

    block_reader = BlockReader(block_data)

    assert block_reader.get(key3, 2) == (value3, 2, True)
    assert block_reader.get(key2, 1) == (value2, 1, False)
    assert block_reader.get(key1, 0) == (value1, 0, False)
    assert last_internal_key == internal_key1


def test_block_is_full_then_finalize_empty():
    block_builder = BlockBuilder(10)

    key, value = b"key1", b"val1"
    internal_key = build_internal_key(key, 0, False)
    block_builder.add(internal_key, value)

    assert block_builder.is_full()
    assert block_builder.last_internal_key == internal_key
    block_builder.finalize()
    assert block_builder.is_empty()
    assert block_builder.last_internal_key == b""


def test_block_add_empty_value_is_valid():
    block_builder = BlockBuilder(1024)

    key, value = b"key1", b""
    internal_key = build_internal_key(key, 0, False)
    block_builder.add(internal_key, value)
    block_data, last_internal_key = block_builder.finalize()

    assert BlockReader(block_data).get(key, 0) == (value, 0, False)
    assert last_internal_key == internal_key


def test_block_reader_get_non_existing_key():
    block_builder = BlockBuilder(1024)

    key, value = b"key1", b"val1"
    internal_key = build_internal_key(key, 0, False)
    block_builder.add(internal_key, value)

    block_data, _ = block_builder.finalize()
    block_reader = BlockReader(block_data)

    assert block_reader.get(key, 0) == (value, 0, False)
    assert block_reader.get(b"key2", 1) == (None, None, False)
