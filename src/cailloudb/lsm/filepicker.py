from constants import (
    SSTABLE_L0_COMPACTION_TRIGGER,
    SSTABLE_L1_MAX_BYTES,
    SSTABLE_LEVEL_MULTIPLIER,
)

from custom_types import FileMetaData


class FilePicker:
    """
    Selects compaction levels/files and applies compaction metadata updates.
    """

    _levels: list[list[FileMetaData]]
    _max_levels: int

    def __init__(self, levels: list[list[FileMetaData]], max_levels: int):
        self._levels = levels
        self._max_levels = max_levels

    def pick_compaction_level(self) -> int | None:
        """
        Find which level to compact.
        """
        if len(self._levels) < 2:
            return None

        # Priority is l0
        if len(self._levels[0]) >= SSTABLE_L0_COMPACTION_TRIGGER:
            return 0

        # Discover other levels compaction needs
        max_bytes = SSTABLE_L1_MAX_BYTES
        # Last level cannot be source because we compact source -> source + 1.
        source_levels_count = min(self._max_levels, len(self._levels)) - 1
        for level_idx in range(1, source_levels_count):
            current_level_bytes = sum(
                meta["file_size"] for meta in self._levels[level_idx]
            )
            if current_level_bytes > max_bytes:
                return level_idx
            max_bytes *= SSTABLE_LEVEL_MULTIPLIER

        return None

    def pick_compaction_files(
        self, source_level: int, target_level: int
    ) -> tuple[list[FileMetaData], list[FileMetaData]]:
        """
        Select files from source level and overlapping files from target level.
        """
        source_files: list[FileMetaData]
        target_files: list[FileMetaData]

        if source_level == 0:
            # L0 has overlapping keys natively, safest to grab them all
            # to ensure we don't violate sequence order.
            source_files = self._levels[0][:]
            # Keep existing behavior: compact all files from L1 together with L0.
            target_files = self._levels[target_level][:]
        else:
            # For L1+, pick the first file that caused the overflow.
            # (A production DB tracks a round-robin cursor here).
            if not self._levels[source_level]:
                return [], []
            source_files = [self._levels[source_level][0]]
            target_files = []

        if not source_files:
            return [], []

        if source_level == 0:
            return source_files, target_files

        # Determine the global min and max keys for the chosen source files.
        min_key = min(meta["smallest_key"] for meta in source_files)
        max_key = max(meta["largest_key"] for meta in source_files)

        # Find all files in the target level that overlap with this range.
        for meta in self._levels[target_level]:
            if (
                meta["smallest_key"] <= max_key
                and meta["largest_key"] >= min_key
            ):
                target_files.append(meta)

        return source_files, target_files

    def apply_compaction_result(
        self,
        source_level: int,
        target_level: int,
        old_source_files: list[FileMetaData],
        old_target_files: list[FileMetaData],
        new_files: list[FileMetaData],
    ) -> None:
        """Atomically swap out compacted files and insert new compacted files."""

        # Remove compacted files from source level.
        self._levels[source_level] = [
            meta for meta in self._levels[source_level] if meta not in old_source_files
        ]

        # Remove compacted files from target level.
        self._levels[target_level] = [
            meta for meta in self._levels[target_level] if meta not in old_target_files
        ]

        # Add newly created files to target level.
        self._levels[target_level].extend(new_files)

        # L1+ levels must remain sorted by key range.
        self._levels[target_level].sort(key=lambda meta: meta["smallest_key"])
