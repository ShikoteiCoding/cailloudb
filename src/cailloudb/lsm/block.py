import io
from typing import Iterator

from constants import (
    INTERNAL_KEY_LEN_STRUCT,
    KEY_METADATA_STRUCT,
    VALUE_LEN_STRUCT,
    VALUE_TYPE_DELETE,
)
from lsm.utils import extract_from_internal_key


class BlockBuilder:
    """
    SSTable Data Block Writer.
    """

    #: Target block size (can be spilled over)
    target_block_size: int

    #: Buffer kept with EOF offset
    buffer: io.BytesIO

    #: Last Internal Key for Sparse Index statistics
    #: For updates, this is the "oldest" because of inversion
    #: IK(b"key1", 0) becomes greater than IK(b"key1", 1)
    last_internal_key: bytes

    def __init__(self, block_size: int):
        self.target_block_size = block_size
        self.buffer = io.BytesIO()
        self.last_internal_key: bytes = b""

    def add(self, internal_key: bytes, value: bytes):
        """
        Packs the key and value into the block buffer.

        Record encoding:
            [Key length][Internal Key][Value length][Value]
        """
        # Write key
        self.buffer.write(INTERNAL_KEY_LEN_STRUCT.pack(len(internal_key)))
        self.buffer.write(internal_key)

        # Write value
        self.buffer.write(VALUE_LEN_STRUCT.pack(len(value)))
        self.buffer.write(value)

        self.last_internal_key = internal_key

    def is_full(self) -> bool:
        """
        Returns True if the block has reached the target size.

        Note: Blocks can be larger than block_size because we
        finish writing a full record before checking this.
        """
        return self.buffer.tell() >= self.target_block_size

    def is_empty(self) -> bool:
        return self.buffer.tell() == 0

    def finalize(self) -> tuple[bytes, bytes]:
        """
        Returns the finalized block data and resets the buffer.
        """
        block_data = self.buffer.getvalue()
        last_internal_key = self.last_internal_key

        self.buffer = io.BytesIO()
        self.last_internal_key = b""

        return block_data, last_internal_key


class BlockReader:
    """
    SSTable Data Block Reader.
    """

    #: One single block of data
    data: bytes

    #: Size of the block
    size: int

    def __init__(self, block_data: bytes):
        self.data = block_data
        self.size = len(block_data)

    def __iter__(self) -> Iterator[tuple[bytes, bytes]]:
        """
        Iterates through the packed records inside this specific block.
        """
        cursor = 0

        while cursor < self.size:
            # Read key length
            key_len = INTERNAL_KEY_LEN_STRUCT.unpack_from(self.data, cursor)[0]
            cursor += INTERNAL_KEY_LEN_STRUCT.size

            # Read key
            internal_key = self.data[cursor : cursor + key_len]
            cursor += key_len

            # Read value length
            val_len = VALUE_LEN_STRUCT.unpack_from(self.data, cursor)[0]
            cursor += VALUE_LEN_STRUCT.size

            # Read value
            value = self.data[cursor : cursor + val_len]
            cursor += val_len

            yield internal_key, value

    def get(self, key: bytes, seq_num: int) -> tuple[bytes | None, int | None, bool]:
        """
        Scans the block for the latest version of `key` where seq_num <= target seq_num.

        Returns:
            (val, seq_num, False) -> Key found
            (b"", seq_num, True) -> Key found but it is a tombstone
            (None, None, False) -> Key not found in this block
        """
        for internal_key, value in self:
            # Extract table_key
            table_key = internal_key[: -KEY_METADATA_STRUCT.size]

            # Optimization: keys are sorted, if  table_key > target key, it is not in this block.
            if table_key > key:
                return None, None, False

            if table_key == key:
                _, table_seq_num, value_type = extract_from_internal_key(internal_key)

                if table_seq_num <= seq_num:
                    if value_type == VALUE_TYPE_DELETE:
                        return b"", table_seq_num, True
                    return value, table_seq_num, False

        return None, None, False
