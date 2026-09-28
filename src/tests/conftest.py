from pathlib import Path

import pytest

TEST_DB = "test-db"

# InMemoryStore writes here unless a test passes another path
_WAL_PATH = Path("cailloudb-data") / "wal"


@pytest.fixture(autouse=True)
def reset_wal_file():
    """Truncate the default WAL file between tests."""
    _WAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    _WAL_PATH.write_bytes(b"")
    yield
    _WAL_PATH.write_bytes(b"")
