import struct
from dataclasses import dataclass
from pathlib import Path

WAL_HEADER_SIZE = 7
WAL_SINGLE_KIND = 0
WAL_BATCH_KIND_FULL = 1
WAL_BATCH_KIND_FIRST = 2
WAL_BATCH_KIND_MIDDLE = 3
WAL_BATCH_KIND_LAST = 4

INTERNAL_KEY_VALUE_TYPE_DELETE = 0x0
INTERNAL_KEY_VALUE_TYPE_PUT = 0x1
INTERNAL_KEY_MAX_SEQ_NUM = (1 << 56) - 1
INTERNAL_KEY_LEN_METADATA = 8

SSTABLE_MAGIC_NUMBER = 0x4341494C

STRUCT_WAL_CRC = struct.Struct(">I")
STRUCT_WAL_PAYLOAD_LEN = struct.Struct(">H")
STRUCT_WAL_SEQ = struct.Struct(">Q")
STRUCT_WAL_LEN = struct.Struct(">I")

STRUCT_INTERNAL_KEY_LEN = struct.Struct("<I")
STRUCT_INTERNAL_KEY_METADATA = struct.Struct(">Q")

STRUCT_SSTABLE_FOOTER = struct.Struct(">QQI")
STRUCT_SSTABLE_INDEX_OFFSET_LEN = struct.Struct(">QQ")
STRUCT_SSTABLE_BLOCK_VALUE_LEN = struct.Struct("<I")

TOMBSTONE = b"__INTERNAL_TOMBSTONE_MARKER__"

@dataclass(slots=True)
class Settings:
    default_wal_dir: Path = Path("/tmp/cailloudb-data") / "wal"
    default_sstable_dir: Path = Path("/tmp/cailloudb-data") / "sstables"
    
    # Memory & Buffering
    memtable_max_bytes_size: int = 32 * 1024 * 1024  # 32 MB
    wal_block_size: int = 32 * 1024                 # 32 KB
    sstable_block_default_size: int = 4096          # 4 KB
    
    # SSTable & Compaction Limits
    sstable_max_file_size: int = 64 * 1024 * 1024   # 64 MB
    sstable_max_levels: int = 7
    sstable_l0_compaction_trigger: int = 4
    sstable_l1_max_bytes: int = 10 * 1024 * 1024    # 10 MB
    sstable_level_multiplier: int = 10