from settings import Settings
from custom_types import FileMetaData


class FilePicker:
    """
    Selects compaction levels/files and applies compaction metadata updates.

    NOTE: Currently very naive algorithm, should be further improved.
    """

    #: Reference to LSMTree levels
    _levels: list[list[FileMetaData]]

    #: LSMTree max level
    _max_levels: int

    #: L0 compaction trigger
    _sstable_l0_compaction_trigger: int

    #: L1 max bytes budget
    _sstable_l1_max_bytes: int

    #: Per-level bytes multiplier
    _sstable_level_multiplier: int

    def __init__(
        self,
        levels: list[list[FileMetaData]],
        max_levels: int,
        settings: Settings | None = None,
        *,
        sstable_l0_compaction_trigger: int | None = None,
        sstable_l1_max_bytes: int | None = None,
        sstable_level_multiplier: int | None = None,
    ):
        if settings is not None and any(
            v is not None
            for v in (
                sstable_l0_compaction_trigger,
                sstable_l1_max_bytes,
                sstable_level_multiplier,
            )
        ):
            raise ValueError(
                "Provide either `settings` or explicit compaction args, not both"
            )

        resolved_settings = settings or Settings()

        self._levels = levels
        self._max_levels = max_levels
        self._sstable_l0_compaction_trigger = (
            resolved_settings.sstable_l0_compaction_trigger
            if sstable_l0_compaction_trigger is None
            else sstable_l0_compaction_trigger
        )
        self._sstable_l1_max_bytes = (
            resolved_settings.sstable_l1_max_bytes
            if sstable_l1_max_bytes is None
            else sstable_l1_max_bytes
        )
        self._sstable_level_multiplier = (
            resolved_settings.sstable_level_multiplier
            if sstable_level_multiplier is None
            else sstable_level_multiplier
        )

        if self._sstable_l0_compaction_trigger < 1:
            raise ValueError("`sstable_l0_compaction_trigger` must be >= 1")
        if self._sstable_l1_max_bytes < 1:
            raise ValueError("`sstable_l1_max_bytes` must be >= 1")
        if self._sstable_level_multiplier < 1:
            raise ValueError("`sstable_level_multiplier` must be >= 1")

    def pick_compaction_level(self) -> int:
        """
        Find which level to compact.
        """
        if self._max_levels < 2:
            return -1

        # Priority is l0
        l0_trigger = self._sstable_l0_compaction_trigger
        if len(self._levels[0]) >= l0_trigger:
            return 0

        max_bytes = self._sstable_l1_max_bytes
        level_multiplier = self._sstable_level_multiplier
        source_levels_count = min(self._max_levels, len(self._levels)) - 1
        for level_idx in range(1, source_levels_count):
            current_level_bytes = sum(
                meta["file_size"] for meta in self._levels[level_idx]
            )
            if current_level_bytes > max_bytes:
                return level_idx
            max_bytes *= level_multiplier

        return -1

    def pick_compaction_files(
        self, source_level: int, target_level: int
    ) -> tuple[list[FileMetaData], list[FileMetaData]]:
        """
        Select files from source level and overlapping files from target level.
        """
        source_files: list[FileMetaData]
        target_files: list[FileMetaData]

        if source_level == 0:
            source_files = self._levels[source_level][:]
            target_files = self._levels[target_level][:]
        else:
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
            if meta["smallest_key"] <= max_key and meta["largest_key"] >= min_key:
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
        """
        Atomically swap out compacted files and insert new compacted files.
        """

        # Remove compacted files from source & target
        self._levels[source_level] = [
            meta for meta in self._levels[source_level] if meta not in old_source_files
        ]
        self._levels[target_level] = [
            meta for meta in self._levels[target_level] if meta not in old_target_files
        ]

        # Update target level + keep ordering
        self._levels[target_level].extend(new_files)
        self._levels[target_level].sort(key=lambda meta: meta["smallest_key"])
