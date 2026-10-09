import pytest

from cailloudb.lsm.block import BlockBuilder, BlockReader
from cailloudb.lsm.utils import build_internal_key

__all__ = ["test_block_encode_and_decode"]


def test_block_encode_and_decode():
    block_builder = BlockBuilder(1024)

    key = b"key1"
    internal_key = build_internal_key(key, 0, False)
    value = b"val1"

    block_builder.add(internal_key, value)
    block_data, last_internal_key = block_builder.reset()

    table_value, table_seq_num, is_tombstone = BlockReader(block_data).get(key, 0)
    assert not is_tombstone
    assert table_seq_num == 0
    assert value == table_value
    assert internal_key == last_internal_key
