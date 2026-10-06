from typing import Literal

from constants import (
    KEY_METADATA_STRUCT,
    MAX_SEQ_NUM,
    VALUE_TYPE_DELETE,
    VALUE_TYPE_PUT,
)


def build_internal_key(key: bytes, seq_num: int, is_delete: bool) -> bytes:
    """
    Build an internal key used in `cailloudb.lsm.skiplist` and `cailloudb.lsm.sstable`
    """
    inverted_seq = MAX_SEQ_NUM - seq_num
    value_type = VALUE_TYPE_DELETE if is_delete else VALUE_TYPE_PUT
    metadata_int = (inverted_seq << 8) | value_type
    metadata_bytes = metadata_int.to_bytes(8, byteorder="big")

    return key + metadata_bytes


def extract_from_internal_key(
    internal_key: bytes,
) -> tuple[bytes, int, Literal[0] | Literal[1]]:
    table_key = internal_key[: -KEY_METADATA_STRUCT.size]
    # Unpack metadata to get sequence number and type
    metadata_bytes = internal_key[-KEY_METADATA_STRUCT.size :]
    key_metadata = KEY_METADATA_STRUCT.unpack(metadata_bytes)[0]

    inverted_seq = key_metadata >> 8
    table_seq_num = MAX_SEQ_NUM - inverted_seq
    value_type = key_metadata & 0xFF

    return table_key, table_seq_num, value_type
