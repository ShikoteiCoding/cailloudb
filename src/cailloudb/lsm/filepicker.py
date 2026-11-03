from custom_types import FileMetadata
from lsm.version import Version


class FilePicker:
    """
    Selects compaction levels/files and applies compaction metadata updates.

    NOTE: Currently very naive algorithm, should be further improved.
    """

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
        max_levels: int,
        sstable_l0_compaction_trigger: int,
        sstable_l1_max_bytes: int,
        sstable_level_multiplier: int,
    ):
        self._max_levels = max_levels
        self._sstable_l0_compaction_trigger = sstable_l0_compaction_trigger
        self._sstable_l1_max_bytes = sstable_l1_max_bytes
        self._sstable_level_multiplier = sstable_level_multiplier

    def pick_compaction_level(self, version: Version) -> int:
        """
        Find which level to compact.
        """
        if self._max_levels < 2:
            return -1

        levels = version.get_levels()

        # Priority is l0
        l0_trigger = self._sstable_l0_compaction_trigger
        if len(levels[0]) >= l0_trigger:
            return 0

        max_bytes = self._sstable_l1_max_bytes
        level_multiplier = self._sstable_level_multiplier
        source_levels_count = min(self._max_levels, len(levels)) - 1
        for level_idx in range(1, source_levels_count):
            current_level_bytes = sum(meta["file_size"] for meta in levels[level_idx])
            if current_level_bytes > max_bytes:
                return level_idx
            max_bytes *= level_multiplier

        return -1

    def pick_compaction_files(
        self, source_level: int, target_level: int, version: Version
    ) -> tuple[list[FileMetadata], list[FileMetadata]]:
        """
        Select files from source level and overlapping files from target level.

        NOTE: With current algorithm, source levels are ALL selected for compaction
        while target level can have 0 or more file selected depending on overlapping.
        """
        source_files: list[FileMetadata]
        target_files: list[FileMetadata]
        levels = version.get_levels()

        if source_level == 0:
            source_files = levels[source_level][:]
            target_files = levels[target_level][:]
        else:
            if not levels[source_level]:
                return [], []
            source_files = [levels[source_level][0]]
            target_files = []

        if not source_files:
            return [], []

        if source_level == 0:
            return source_files, target_files

        # Determine the global min and max keys for the chosen source files.
        min_key = min(meta["smallest_key"] for meta in source_files)
        max_key = max(meta["largest_key"] for meta in source_files)

        # Find all files in the target level that overlap with this range.
        for meta in levels[target_level]:
            if meta["smallest_key"] <= max_key and meta["largest_key"] >= min_key:
                target_files.append(meta)

        return source_files, target_files
