from pathlib import Path

TOMBSTONE = b"__INTERNAL_TOMBSTONE_MARKER__"
DEFAULT_WAL = Path("/tmp/cailloudb-data") / "wal"
