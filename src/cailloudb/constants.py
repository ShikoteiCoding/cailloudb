from pathlib import Path

TOMBSTONE = b"__INTERNAL_TOMBSTONE_MARKER__"
DEFAULT_WAL = Path("cailloudb-data") / "wal"
