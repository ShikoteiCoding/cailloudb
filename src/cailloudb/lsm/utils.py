from typing import Literal

from settings import (
    INTERNAL_KEY_MAX_SEQ_NUM,
    STRUCT_INTERNAL_KEY_METADATA,
    INTERNAL_KEY_VALUE_TYPE_DELETE,
    INTERNAL_KEY_VALUE_TYPE_PUT,
)


def build_internal_key(key: bytes, seq_num: int, is_delete: bool) -> bytes:
    """
    Build an internal key used in `cailloudb.lsm.skiplist` and `cailloudb.lsm.sstable`.

    Internal_key always follow:
        [ Key ][ ~SeqNum ][ ValueType ]

    ValueType: PUT or DELETE
    """
    inverted_seq = INTERNAL_KEY_MAX_SEQ_NUM - seq_num
    value_type = (
        INTERNAL_KEY_VALUE_TYPE_DELETE if is_delete else INTERNAL_KEY_VALUE_TYPE_PUT
    )
    metadata_int = (inverted_seq << 8) | value_type
    metadata_bytes = metadata_int.to_bytes(8, byteorder="big")

    return key + metadata_bytes


def extract_from_internal_key(
    internal_key: bytes,
) -> tuple[bytes, int, Literal[0] | Literal[1]]:
    """
    Extract the key, seq_num and value_type from an internal key.

    Internal_key always follow:
        [ Key ][ ~SeqNum ][ ValueType ]

    ValueType: PUT or DELETE
    """
    table_key = internal_key[: -STRUCT_INTERNAL_KEY_METADATA.size]
    # Unpack metadata to get sequence number and value type
    metadata_bytes = internal_key[-STRUCT_INTERNAL_KEY_METADATA.size :]
    key_metadata = STRUCT_INTERNAL_KEY_METADATA.unpack(metadata_bytes)[0]

    inverted_seq = key_metadata >> STRUCT_INTERNAL_KEY_METADATA.size
    table_seq_num = INTERNAL_KEY_MAX_SEQ_NUM - inverted_seq
    value_type = key_metadata & 0xFF

    return table_key, table_seq_num, value_type
