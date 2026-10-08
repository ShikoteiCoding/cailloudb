import io

from constants import (
    INTERNAL_KEY_LEN_STRUCT,
    KEY_METADATA_STRUCT,
    MAX_SEQ_NUM,
    VALUE_LEN_STRUCT,
)


class BlockBuilder:
    """
    SSTable Data Block writer.
    """

    #: Target block size (can be spilled over)
    target_block_size: int

    #: Buffer kept with EOF offset
    buffer: io.BytesIO

    #: Last Internal Key for Sparse Index statistics
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
        # 1. Write Key Length + Key
        self.buffer.write(INTERNAL_KEY_LEN_STRUCT.pack(len(internal_key)))
        self.buffer.write(internal_key)

        # 2. Write Value Length + Value
        self.buffer.write(VALUE_LEN_STRUCT.pack(len(value)))
        self.buffer.write(value)

        # 3. Update the last key (used by the writer for the Sparse Index)
        self.last_internal_key = internal_key

    def is_full(self) -> bool:
        """
        Returns True if the block has reached the target size.

        Note: Blocks will be slightly larger than block_size because we
        finish writing a full record before checking this.
        """
        return self.buffer.tell() >= self.target_block_size

    def is_empty(self) -> bool:
        return self.buffer.tell() == 0

    def reset(self) -> tuple[bytes, bytes]:
        """
        Returns the serialized block data and resets the buffer.
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

    def __iter__(self):
        """
        Iterates through the packed records inside this specific block.
        """
        cursor = 0

        while cursor < self.size:
            # Read Key Length
            key_len = INTERNAL_KEY_LEN_STRUCT.unpack_from(self.data, cursor)[0]
            cursor += INTERNAL_KEY_LEN_STRUCT.size

            # Read Key
            internal_key = self.data[cursor : cursor + key_len]
            cursor += key_len

            # Read Value Length
            val_len = VALUE_LEN_STRUCT.unpack_from(self.data, cursor)[0]
            cursor += VALUE_LEN_STRUCT.size

            # Read Value
            value = self.data[cursor : cursor + val_len]
            cursor += val_len

            yield internal_key, value

    def get(self, key: bytes, seq_num: int) -> tuple[bytes | None, int | None, bool]:
        """
        Scans the block for the latest version of `key` where seq_num <= query seq_num.

        Returns:
            (val, False) -> Key found
            (None, True) -> Key found but it is a tombstone
            (None, False) -> Key not found in this block
        """
        for internal_key, value in self:
            # Split internal key into user_key and metadata (8 bytes: 7 for seq, 1 for type)
            table_key = internal_key[: -KEY_METADATA_STRUCT.size]

            # Optimization: since keys are sorted, if we encounter a user_key
            # strictly greater than what we want, it doesn't exist in this block.
            if table_key > key:
                return None, None, False

            if table_key == key:
                # Unpack metadata to get sequence number and type
                metadata_bytes = internal_key[-KEY_METADATA_STRUCT.size :]
                key_metadata = KEY_METADATA_STRUCT.unpack(metadata_bytes)[0]

                inverted_seq = key_metadata >> 8
                entry_seq_num = MAX_SEQ_NUM - inverted_seq
                value_type = key_metadata & 0xFF

                # Check snapshot visibility
                if entry_seq_num <= seq_num:
                    if value_type == 0x00:  # Tombstone (Delete)
                        return None, entry_seq_num, True  # Stop searching lower levels!
                    return value, entry_seq_num, False

        return None, None, False
