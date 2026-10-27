import os
import struct
from dataclasses import dataclass, fields
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
    """
    Runtime settings (hold only what should be configurable by the user).
    """

    # Usage architectural guardrail:
    # - If a class distributes settings to other classes, pass it Settings.
    # - If a class is an invariant, do not use Settings but constants values above.
    # - If a class is a leaf in the codebase tree, do not pass Settings.
    # - If an object creation is latency sensitive, do not use Settings.

    # Durability
    wal_enabled: bool = True
    default_wal_dir: Path = Path("/tmp/cailloudb-data") / "wal"
    durable_sstable: bool = True
    default_sstable_dir: Path = Path("/tmp/cailloudb-data") / "sstables"

    # Memory & Buffering
    memtable_max_bytes_size: int = 32 * 1024 * 1024  # 32 MB
    wal_block_size: int = 32 * 1024  # 32 KB
    sstable_block_default_size: int = 4096  # 4 KB

    # SSTable & Compaction Limits
    sstable_max_file_size: int = 64 * 1024 * 1024  # 64 MB
    sstable_max_levels: int = 7
    sstable_l0_compaction_trigger: int = 4
    sstable_l1_max_bytes: int = 10 * 1024 * 1024  # 10 MB
    sstable_level_multiplier: int = 10

    # Background processes
    flush_enabled: bool = True
    compaction_enabled: bool = True
    compaction_interval_s: int = 10

    def __post_init__(self) -> None:
        for field in fields(self):
            env_name = field.name.upper()
            env_value = os.getenv(env_name)
            if env_value is None:
                continue

            current_value = getattr(self, field.name)
            converted_value = self._convert_env_value(
                env_name=env_name,
                raw_value=env_value,
                current_value=current_value,
            )
            setattr(self, field.name, converted_value)

        self._validate()

    def _validate(self) -> None:
        if self.wal_block_size <= WAL_HEADER_SIZE:
            raise ValueError("`wal_block_size` must be greater than WAL_HEADER_SIZE")
        if self.sstable_l0_compaction_trigger < 1:
            raise ValueError("`sstable_l0_compaction_trigger` must be >= 1")
        if self.sstable_l1_max_bytes < 1:
            raise ValueError("`sstable_l1_max_bytes` must be >= 1")
        if self.sstable_level_multiplier < 1:
            raise ValueError("`sstable_level_multiplier` must be >= 1")

    @staticmethod
    def _convert_env_value(
        env_name: str, raw_value: str, current_value: object
    ) -> object:
        if isinstance(current_value, bool):
            normalized = raw_value.strip().lower()
            if normalized in {"1", "true", "True"}:
                return True
            if normalized in {"0", "false", "False"}:
                return False
            raise ValueError(f"Invalid boolean value for {env_name}: {raw_value!r}")

        if isinstance(current_value, int):
            try:
                return int(raw_value)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid integer value for {env_name}: {raw_value!r}"
                ) from exc

        if isinstance(current_value, Path):
            return Path(raw_value)

        return raw_value
