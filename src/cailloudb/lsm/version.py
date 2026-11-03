import io
import json
import os
import threading
from pathlib import Path
from typing import Iterator

from custom_types import SSTableEntry
from lsm.sstable import DurableSSTable, SSTable
from lsm.table_builder import FileMetadata
from lsm.utils import extract_from_internal_key
from settings import TOMBSTONE


class VersionEdit:
    """
    Atomic structural change of the LSMTree.

    Committed to MANIFEST changelog.
    """

    #: List of deleted files
    deleted_files: list[tuple[int, int]]

    #: List of added files
    added_files: list[tuple[int, FileMetadata]]

    #: Metadata
    next_file_id: int
    log_number: int

    def __init__(
        self,
        deleted_files: list[tuple[int, int]] | None = None,
        added_files: list[tuple[int, FileMetadata]] | None = None,
        next_file_id: int = -1,
        log_number: int = -1,
    ):
        self.deleted_files = deleted_files if deleted_files is not None else []
        self.added_files = added_files if added_files is not None else []
        self.next_file_id = next_file_id
        self.log_number = log_number

    def add_file(self, level: int, file_meta: FileMetadata) -> None:
        self.added_files.append((level, file_meta))

    def delete_file(self, level: int, file_id: int) -> None:
        self.deleted_files.append((level, file_id))

    def encode(self) -> bytes:
        """
        Serializes the version edit into a single line of JSON bytes.
        """
        data = {
            "deleted_files": str(self.deleted_files),
            "added_files": str(self.added_files),
            "next_file_id": str(self.next_file_id),
            "log_number": str(self.log_number),
        }
        return json.dumps(data).encode("utf-8") + b"\n"

    @classmethod
    def decode(cls, line: bytes) -> VersionEdit:
        data = json.loads(line.decode("utf-8"))
        return cls(
            deleted_files=[(lvl, fid) for lvl, fid in data.get("deleted_files", [])],
            added_files=[(lvl, meta) for lvl, meta in data.get("added_files", [])],
            next_file_id=data.get("next_file_id"),
            log_number=data.get("log_number"),
        )


class Version:
    #: List of levels of SSTable metadata
    #: l0 -> overlapping ranges, update history, ordered by seq_num
    #: l{1...N} -> non overlapping ranges, no history, sorted by min key
    _levels: list[list[FileMetadata]]

    def __init__(self, levels: list[list[FileMetadata]]):
        self._levels = levels

    def find_files_containing(self, key: bytes) -> Iterator[FileMetadata]:
        """
        Find files likely to own the key.
        """
        for level_idx, level_files in enumerate(self._levels):
            if not level_files:
                continue

            if level_idx == 0:
                # Level 0 can overlap, all files must be checked
                for meta in reversed(level_files):
                    # XXX: cpu-costly, should be optimized
                    if (
                        extract_from_internal_key(meta["smallest_key"])[0]
                        <= key
                        <= extract_from_internal_key(meta["largest_key"])[0]
                    ):
                        yield meta
            else:
                # Levels > 0 are non-overlapping and sorted by key range.
                for meta in level_files:
                    smallest_key = extract_from_internal_key(meta["smallest_key"])[0]
                    largest_key = extract_from_internal_key(meta["largest_key"])[0]

                    # Exit early because of no overlap property
                    if key < smallest_key:
                        break

                    if key > largest_key:
                        continue

                    yield meta
                    break

    def get_levels(self) -> list[list[FileMetadata]]:
        return self._levels


class VersionSet:
    """
    Manifest-backed state structure for LSMTree.
    """

    #: Path to metadata
    _path: Path

    #: Lock for manifest file edit
    _version_lock: threading.Lock

    #: Pointer to writeable MANIFEST file
    _manifest_file: io.BufferedWriter
    _manifest_file_id: int

    #: Current Metadata version
    _current_version: Version

    #: SSTables map from "file_id" to SSTable pointer
    _sstables_map: dict[int, SSTable]

    #: Next file id for newly created SSTables
    _next_file_id: int

    #: File ID Lock
    _file_id_lock: threading.Lock

    def __init__(self, path: Path, max_levels: int):
        self._path = path
        self._version_lock = threading.Lock()
        self._manifest_file_id = 1
        self._sstables_map = {}
        self._next_file_id = 1
        self._file_id_lock = threading.Lock()

        self.init(max_levels)

    def init(self, max_levels: int) -> None:
        self._next_file_id = self._recover_next_file_id(self._path)
        current_path = self._path / "CURRENT"
        levels = [[] for _ in range(max_levels)]

        if current_path.exists():
            levels = self._recover_from_manifest(max_levels)
            self._manifest_file_id = self._read_current_manifest_id()
            manifest_path = self._manifest_path_from_id(self._manifest_file_id)
            self._manifest_file = open(manifest_path, "ab")
        else:
            self._create_initial_manifest(1)

        self._current_version = Version(levels)

    @staticmethod
    def _recover_next_file_id(sstable_dir: Path) -> int:
        """
        Return a file id strictly greater than any existing .sst file name.
        """
        max_file_id = 0

        for path in sstable_dir.glob("*.sst"):
            stem = path.stem
            if stem.isdigit():
                max_file_id = max(max_file_id, int(stem))

        return max_file_id + 1

    def file_id_generator(self) -> int:
        """
        Thread-safe file ID Generator.

        XXX: Should be moved to FileManager / VersionManager when introduced.
        """
        with self._file_id_lock:
            file_id = self._next_file_id
            self._next_file_id += 1
            return file_id

    def _create_initial_manifest(self, starting_manifest_file_id: int) -> None:
        """
        Writes the first MANIFEST-000001 and points CURRENT to it.
        """
        self._manifest_file_id = starting_manifest_file_id
        manifest_path = self._path / f"MANIFEST-{self._manifest_file_id:06d}"
        current_path = self._path / "CURRENT"
        manifest_path.touch()
        current_path.touch()

        with open(current_path, "wb") as current_f:
            current_f.write(str(self._manifest_file_id).encode("utf-8"))
            current_f.flush()
            os.fsync(current_f.fileno())

        self._manifest_file = open(manifest_path, "ab")

    def _read_current_manifest_id(self) -> int:
        current_path = self._path / "CURRENT"
        current_data = current_path.read_text(encoding="utf-8").strip()
        return int(current_data) if current_data else 1

    def _manifest_path_from_id(self, manifest_id: int) -> Path:
        return self._path / f"MANIFEST-{manifest_id:06d}"

    def log_and_apply(self, edit: VersionEdit, sstables: list[SSTable]) -> None:
        with self._version_lock:
            self._write_to_manifest(edit)
            updated_levels = self._compute_levels(edit, sstables)
            self._current_version = Version(updated_levels)

    def _write_to_manifest(self, edit: VersionEdit) -> None:
        """
        Write a version change to the active manifest file.
        """
        if not self._manifest_file:
            raise RuntimeError("Manifest file handle is not open or file is missing.")

        encoded_data = edit.encode()
        self._manifest_file.write(encoded_data)
        self._manifest_file.flush()
        os.fsync(self._manifest_file.fileno())

    def _compute_levels(
        self,
        edit: VersionEdit,
        sstables: list[SSTable],  # XXX: Remove sstables
    ) -> list[list[FileMetadata]]:
        """
        Recompute levels from current levels and a version change.
        """
        current_levels = self._current_version.get_levels()
        updated_levels = [list(level_files) for level_files in current_levels]

        for level, file_id in edit.deleted_files:
            updated_levels[level] = [
                fm for fm in updated_levels[level] if fm["file_id"] != file_id
            ]
            self._sstables_map.pop(file_id, None)

        for level, file_meta in edit.added_files:
            updated_levels[level].append(file_meta)
            file_id = file_meta["file_id"]

        # XXX: Remove sstables
        for sstable in sstables:
            self._sstables_map[sstable.file_id] = sstable

        self._next_file_id = max(self._next_file_id, edit.next_file_id)
        return updated_levels

    def _recover_from_manifest(self, max_levels: int) -> list[list[FileMetadata]]:
        levels = [[] for _ in range(max_levels)]

        manifest_id = self._read_current_manifest_id()
        manifest_path = self._manifest_path_from_id(manifest_id)

        with manifest_path.open("rb") as f:
            for line in f:
                edit = VersionEdit.decode(line)

                for level, file_id in edit.deleted_files:
                    levels[level] = [
                        fm for fm in levels[level] if fm["file_id"] != file_id
                    ]
                    self._sstables_map.pop(file_id, None)

                for level, file_meta in edit.added_files:
                    levels[level].append(file_meta)
                    file_id = file_meta["file_id"]
                    self._sstables_map[file_id] = DurableSSTable(
                        file_id, self._path, f"{file_id:06d}.sst"
                    )

                self._next_file_id = max(self._next_file_id, edit.next_file_id)

        return levels

    def get(self, key: bytes, seq_num: int) -> bytes | None:
        version = self._current_version
        candidate_files = version.find_files_containing(key)

        for file_meta in candidate_files:
            file_id = file_meta["file_id"]
            sstable = self._sstables_map.get(file_id)

            if sstable is not None:
                if entry := sstable.get(key, seq_num):
                    if entry:
                        return None if entry["value"] == TOMBSTONE else entry["value"]

        return None

    def scan(
        self, start_key: bytes | None, end_key: bytes | None, seq_num: int
    ) -> Iterator[SSTableEntry]:
        levels = self._current_version.get_levels()

        for level_idx, level_files in enumerate(levels):
            for meta in level_files:
                # XXX: cpu-costly, should be optimized
                file_smallest_key = extract_from_internal_key(meta["smallest_key"])[0]
                file_largest_key = extract_from_internal_key(meta["largest_key"])[0]

                if start_key is not None and file_largest_key < start_key:
                    continue

                if end_key is not None and file_smallest_key >= end_key:
                    # Continue on level 0 as key overlap will exist
                    if level_idx == 0:
                        continue
                    # Exit loop as level n+1 are non overlapping and sorted
                    else:
                        break

                sst = self._sstables_map[meta["file_id"]]
                for entry in sst.scan(start_key, end_key, seq_num):
                    yield entry

    def get_current_version(self) -> Version:
        return self._current_version

    def get_sstable_map(self) -> dict[int, SSTable]:
        return self._sstables_map
