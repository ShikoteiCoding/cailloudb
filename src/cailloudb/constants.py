import struct
from pathlib import Path

TOMBSTONE = b"__INTERNAL_TOMBSTONE_MARKER__"
DEFAULT_WAL = Path("/tmp/cailloudb-data") / "wal"
MEMTABLE_MAX_BYTES_SIZE = 32 * 1024 * 1024

_LEN_KEY_LEN = 4
_KEY_LEN_STRUCT = struct.Struct("<I")

_LEN_VAL_LEN = 4
_VAL_LEN_STRUCT = struct.Struct("<I")

_LEN_SEQUENCE_NUM = 8
_SEQ_STRUCT = struct.Struct("<Q")

_LEN_DELETED = 1
_DELETED_STRUCT = struct.Struct("B")
