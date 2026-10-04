import struct
from pathlib import Path

TOMBSTONE = b"__INTERNAL_TOMBSTONE_MARKER__"
DEFAULT_WAL = Path("/tmp/cailloudb-data") / "wal"
MEMTABLE_MAX_BYTES_SIZE = 32 * 1024 * 1024

LEN_KEY_LEN = 4
KEY_LEN_STRUCT = struct.Struct("<I")

LEN_KEY_METADATA = 8
KEY_METADATA_STRUCT = struct.Struct(">Q")

LEN_VAL_LEN = 4
VAL_LEN_STRUCT = struct.Struct("<I")

LEN_SEQUENCE_NUM = 8
SEQ_STRUCT = struct.Struct("<Q")

LEN_DELETED = 1
DELETED_STRUCT = struct.Struct("B")
