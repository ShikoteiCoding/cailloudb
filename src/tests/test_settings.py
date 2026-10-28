from pathlib import Path

import pytest

from cailloudb.settings import WAL_HEADER_SIZE, Settings

__all__ = [
    "test_settings_reads_environment_overrides",
    "test_environment_overrides_constructor_arguments",
    "test_settings_rejects_invalid_boolean_environment_value",
    "test_settings_rejects_invalid_integer_environment_value",
    "test_settings_rejects_invalid_sstable_l0_compaction_trigger",
    "test_settings_rejects_invalid_sstable_l1_max_bytes",
    "test_settings_rejects_invalid_sstable_level_multiplier",
    "test_settings_rejects_invalid_wal_block_size",
]


def test_settings_reads_environment_overrides(monkeypatch):
    monkeypatch.setenv("DEFAULT_WAL_DIR", "/tmp/custom-wal")
    monkeypatch.setenv("MEMTABLE_MAX_BYTES_SIZE", "2048")
    monkeypatch.setenv("COMPACTION_ENABLED", "false")

    settings = Settings()

    assert settings.default_wal_dir == Path("/tmp/custom-wal")
    assert settings.memtable_max_bytes_size == 2048
    assert settings.compaction_enabled is False


def test_environment_overrides_constructor_arguments(monkeypatch):
    monkeypatch.setenv("COMPACTION_INTERVAL_S", "42")

    settings = Settings(compaction_interval_s=5)

    assert settings.compaction_interval_s == 42


def test_settings_rejects_invalid_boolean_environment_value(monkeypatch):
    monkeypatch.setenv("FLUSH_ENABLED", "maybe")

    with pytest.raises(ValueError, match="Invalid boolean value"):
        Settings()


def test_settings_rejects_invalid_sstable_l0_compaction_trigger():
    with pytest.raises(
        ValueError, match="`sstable_l0_compaction_trigger` must be >= 1"
    ):
        Settings(sstable_l0_compaction_trigger=0)


def test_settings_rejects_invalid_sstable_l1_max_bytes():
    with pytest.raises(ValueError, match="`sstable_l1_max_bytes` must be >= 1"):
        Settings(sstable_l1_max_bytes=0)


def test_settings_rejects_invalid_sstable_level_multiplier():
    with pytest.raises(ValueError, match="`sstable_level_multiplier` must be >= 1"):
        Settings(sstable_level_multiplier=0)
