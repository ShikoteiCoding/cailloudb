import json
from pathlib import Path

import pytest

from cailloudb.lsm.custom_types import FileMetadata
from cailloudb.lsm.sstable import SSTable
from cailloudb.lsm.table_builder import write_iterator_to_sstables
from cailloudb.lsm.utils import build_internal_key
from cailloudb.lsm.version import Version, VersionEdit, VersionSet
from cailloudb.settings import TOMBSTONE

__all__ = [
    "test_version_edit_default_lists_are_not_shared",
    "test_version_edit_add_and_delete_file",
    "test_version_edit_decode_from_manifest",
    "test_version_find_files_containing_handles_l0_overlap_in_reverse_order",
    "test_version_find_files_containing_level_gt_zero_stops_early",
    "test_versionset_init_creates_manifest_and_current_and_recovers_next_file_id",
    "test_versionset_init_with_existing_metadata_layout_opens_active_manifest",
    "test_versionset_recover_from_manifest_applies_deletes_and_next_file_id",
    "test_versionset_recover_from_manifest_builds_durable_sstable_map_from_added_files",
    "test_versionset_file_id_generator_is_monotonic",
    "test_versionset_log_and_apply_updates_levels_and_map_and_supports_deletes",
    "test_versionset_write_to_manifest_raises_when_handle_missing",
    "test_versionset_get_and_scan_functional_flow",
]


class FakeSSTable(SSTable):
    def __init__(self, file_id: int, entries: list[dict]):
        self.file_id = file_id
        self._entries = entries

    def get(self, key: bytes, seq_num: int):
        for entry in self._entries:
            if entry["key"] == key and entry["seq_num"] <= seq_num:
                return entry
        return None

    def scan(self, start_key: bytes | None, end_key: bytes | None, seq_num: int):
        for entry in self._entries:
            key = entry["key"]
            if entry["seq_num"] > seq_num:
                continue
            if start_key is not None and key < start_key:
                continue
            if end_key is not None and key >= end_key:
                continue
            yield entry


def _meta(file_id: int, start: bytes, end: bytes) -> FileMetadata:
    return FileMetadata(
        file_id=file_id,
        smallest_key=build_internal_key(start, 0, is_delete=False),
        largest_key=build_internal_key(end, 0, is_delete=False),
        file_size=42,
    )


def test_version_edit_default_lists_are_not_shared():
    # This is to make sure default values are not used here
    edit1 = VersionEdit()
    edit2 = VersionEdit()

    edit1.add_file(0, _meta(1, b"a", b"a"))
    edit1.delete_file(0, 1)

    assert edit1.added_files != []
    assert edit1.deleted_files != []
    assert edit2.added_files == []
    assert edit2.deleted_files == []


def test_version_edit_add_and_delete_file():
    edit = VersionEdit()
    meta = _meta(7, b"k1", b"k9")

    edit.add_file(1, meta)
    edit.delete_file(0, 3)

    assert edit.added_files == [(1, meta)]
    assert edit.deleted_files == [(0, 3)]


@pytest.mark.parametrize(
    "edit",
    [
        VersionEdit(),
        VersionEdit(deleted_files=[(0, 3)], next_file_id=11, log_number=5),
        VersionEdit(
            deleted_files=[(1, 9), (2, 10)],
            added_files=[
                (0, _meta(7, b"a", b"b")),
                (2, _meta(8, b"k1", b"k9")),
            ],
            next_file_id=42,
            log_number=8,
        ),
    ],
)
def test_version_edit_decode_from_manifest(edit: VersionEdit):
    encoded_line = edit.encode()

    assert encoded_line.endswith(b"\n")

    decoded = VersionEdit.decode(encoded_line.splitlines()[0])

    assert decoded.deleted_files == edit.deleted_files
    assert decoded.added_files == edit.added_files
    assert decoded.next_file_id == edit.next_file_id
    assert decoded.log_number == edit.log_number


def test_version_find_files_containing_handles_l0_overlap_in_reverse_order():
    old_meta = _meta(1, b"a", b"z")
    new_meta = _meta(2, b"a", b"z")
    version = Version(levels=[[old_meta, new_meta], []])

    found = list(version.find_files_containing(b"m"))

    assert found == [new_meta, old_meta]


def test_version_find_files_containing_level_gt_zero_stops_early():
    meta1 = _meta(10, b"c", b"f")
    meta2 = _meta(11, b"m", b"z")
    version = Version(levels=[[], [meta1, meta2]])

    # key below first file smallest key should break early and return none
    assert list(version.find_files_containing(b"a")) == []

    # key inside first range should return only first matching file
    assert list(version.find_files_containing(b"d")) == [meta1]


def test_versionset_init_creates_manifest_and_current_and_recovers_next_file_id(
    tmp_path: Path,
):
    (tmp_path / "CURRENT").write_text("MANIFEST-000001", encoding="utf-8")
    manifest_path = tmp_path / "MANIFEST-000001"

    edit1 = VersionEdit(next_file_id=4, log_number=1)
    edit2 = VersionEdit(next_file_id=10, log_number=2)
    manifest_path.write_bytes(edit1.encode() + edit2.encode())

    versions = VersionSet(tmp_path, max_levels=3)

    assert (tmp_path / "CURRENT").exists()
    assert (tmp_path / "MANIFEST-000001").exists()
    assert versions._next_sstable_file_id == 10


def test_versionset_init_with_existing_metadata_layout_opens_active_manifest(
    tmp_path: Path,
):
    (tmp_path / "CURRENT").write_text("MANIFEST-000007", encoding="utf-8")
    manifest_path = tmp_path / "MANIFEST-000007"
    manifest_path.write_bytes(
        json.dumps(
            {
                "deleted_files": [],
                "added_files": [],
                "next_file_id": 4,
                "log_number": 1,
                "last_seq_num": 1,
            }
        ).encode("utf-8")
        + b"\n"
    )

    versions = VersionSet(tmp_path, max_levels=2)
    assert versions._next_manifest_file_id == 7

    before = manifest_path.read_bytes()
    versions.log_and_apply(VersionEdit(next_file_id=5, log_number=2), [])
    after = manifest_path.read_bytes()

    assert len(after) > len(before)
    assert versions._next_sstable_file_id == 5


def test_versionset_recover_from_manifest_applies_deletes_and_next_file_id(
    tmp_path: Path,
):
    versions = VersionSet(tmp_path, max_levels=2)

    # Prime map with an existing file id so a delete edit has an observable effect.
    versions.get_sstable_map()[3] = FakeSSTable(3, [])

    (tmp_path / "CURRENT").write_text("1", encoding="utf-8")
    manifest_path = tmp_path / "MANIFEST-000001"
    manifest_lines = [
        {
            "deleted_files": [],
            "added_files": [],
            "next_file_id": 5,
            "log_number": 1,
            "last_seq_num": 1,
        },
        {
            "deleted_files": [[0, 3]],
            "added_files": [],
            "next_file_id": 8,
            "log_number": 2,
            "last_seq_num": 1,
        },
    ]
    manifest_path.write_bytes(
        b"".join(json.dumps(line).encode("utf-8") + b"\n" for line in manifest_lines)
    )

    levels, _, seq_num = versions._recover_from_manifest(manifest_path, max_levels=2)

    assert levels == [[], []]
    assert 3 not in versions.get_sstable_map()
    assert versions._next_sstable_file_id == 8
    assert seq_num == 1


def test_versionset_recover_from_manifest_builds_durable_sstable_map_from_added_files(
    tmp_path: Path,
):
    def _fixed_file_id_generator() -> int:
        return 1

    entries = iter([(build_internal_key(b"k1", 0, is_delete=False), b"v1")])
    write_iterator_to_sstables(
        iterator=entries,
        file_id_generator=_fixed_file_id_generator,
        max_file_size=1024,
        target_block_size=128,
        durable_sstable=True,
        sstable_dir=tmp_path,
    )

    (tmp_path / "CURRENT").write_text("MANIFEST-000001", encoding="utf-8")
    manifest_path = tmp_path / "MANIFEST-000001"
    edit = VersionEdit(next_file_id=4, log_number=1)
    edit.add_file(0, _meta(1, b"k1", b"k1"))
    manifest_path.write_bytes(edit.encode())

    versions = VersionSet(tmp_path, max_levels=2)

    assert len(versions.get_current_version().get_levels()[0]) == 1
    assert 1 in versions.get_sstable_map()
    assert versions.get_sstable_map()[1].file_id == 1
    assert versions._next_sstable_file_id == 4


def test_versionset_file_id_generator_is_monotonic(tmp_path: Path):
    versions = VersionSet(tmp_path, max_levels=2)

    first = versions.file_id_generator()
    second = versions.file_id_generator()
    third = versions.file_id_generator()

    assert first + 1 == second
    assert second + 1 == third


def test_versionset_log_and_apply_updates_levels_and_map_and_supports_deletes(
    tmp_path: Path,
):
    versions = VersionSet(tmp_path, max_levels=3)

    sst1 = FakeSSTable(1, [{"key": b"a", "seq_num": 0, "value": b"v1"}])
    sst2 = FakeSSTable(2, [{"key": b"n", "seq_num": 0, "value": b"v2"}])

    edit_add = VersionEdit(next_file_id=3, log_number=1)
    meta1 = _meta(1, b"a", b"f")
    meta2 = _meta(2, b"m", b"z")
    edit_add.add_file(0, meta1)
    edit_add.add_file(1, meta2)

    versions.log_and_apply(edit_add, [sst1, sst2])

    levels_after_add = versions.get_current_version().get_levels()
    assert levels_after_add[0] == [meta1]
    assert levels_after_add[1] == [meta2]
    assert versions.get_sstable_map()[1] is sst1
    assert versions.get_sstable_map()[2] is sst2

    edit_delete = VersionEdit(next_file_id=3, log_number=2)
    edit_delete.delete_file(0, 1)
    versions.log_and_apply(edit_delete, [])

    levels_after_delete = versions.get_current_version().get_levels()
    assert levels_after_delete[0] == []
    assert 1 not in versions.get_sstable_map()
    assert 2 in versions.get_sstable_map()


def test_versionset_write_to_manifest_raises_when_handle_missing(tmp_path: Path):
    versions = VersionSet(tmp_path, max_levels=2)
    versions._manifest_file.close()
    versions._manifest_file = None  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="Manifest file handle is not open"):
        versions._write_to_manifest(VersionEdit())


def test_versionset_get_and_scan_functional_flow(tmp_path: Path):
    versions = VersionSet(tmp_path, max_levels=3)

    sst1 = FakeSSTable(
        1,
        [
            {"key": b"k1", "seq_num": 0, "value": b"v1"},
            {"key": b"k2", "seq_num": 0, "value": TOMBSTONE},
            {"key": b"k3", "seq_num": 0, "value": b"v3"},
        ],
    )

    edit = VersionEdit(next_file_id=2, log_number=1)
    edit.add_file(1, _meta(1, b"k1", b"k3"))
    versions.log_and_apply(edit, [sst1])
    version = versions.get_current_version()

    assert versions.get(b"k1", seq_num=10, version=version) == b"v1"
    assert versions.get(b"k2", seq_num=10, version=version) is None
    assert versions.get(b"missing", seq_num=10, version=version) is None

    scanned = list(versions.scan(b"k1", b"k4", seq_num=10, version=version))
    assert scanned == [
        {"key": b"k1", "seq_num": 0, "value": b"v1"},
        {"key": b"k2", "seq_num": 0, "value": TOMBSTONE},
        {"key": b"k3", "seq_num": 0, "value": b"v3"},
    ]
