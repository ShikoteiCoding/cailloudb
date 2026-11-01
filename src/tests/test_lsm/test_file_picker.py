from cailloudb.lsm.filepicker import FilePicker
from cailloudb.settings import Settings

__all__ = [
    "test_file_picker_pick_compaction_level_returns_minus_one_when_max_levels_is_lt_2",
    "test_file_picker_pick_compaction_level_returns_minus_one_with_single_level",
    "test_file_picker_pick_compaction_level_returns_minus_one_below_l0_trigger",
    "test_file_picker_pick_compaction_level_returns_l1_on_size_overflow",
    "test_file_picker_pick_compaction_files_empty_source_returns_empty_lists",
    "test_file_picker_pick_compaction_files_l0_empty_source_returns_empty_lists",
    "test_file_picker_pick_compaction_files_non_l0_empty_source_returns_empty_lists",
    "test_file_picker_pick_compaction_files_non_l0_selects_overlapping_targets",
    "test_file_picker_pick_compaction_level_and_files_for_l0",
]

settings = Settings()
SSTABLE_L1_MAX_BYTES = settings.sstable_l1_max_bytes


def test_file_picker_pick_compaction_level_returns_minus_one_when_max_levels_is_lt_2():
    levels = [[]]
    picker = FilePicker(
        levels=levels,
        max_levels=1,
        sstable_l0_compaction_trigger=settings.sstable_l0_compaction_trigger,
        sstable_l1_max_bytes=settings.sstable_l1_max_bytes,
        sstable_level_multiplier=settings.sstable_level_multiplier,
    )

    assert picker.pick_compaction_level() == -1


def test_file_picker_pick_compaction_level_returns_minus_one_with_single_level():
    levels = [[]]
    picker = FilePicker(
        levels=levels,
        max_levels=7,
        sstable_l0_compaction_trigger=settings.sstable_l0_compaction_trigger,
        sstable_l1_max_bytes=settings.sstable_l1_max_bytes,
        sstable_level_multiplier=settings.sstable_level_multiplier,
    )

    assert picker.pick_compaction_level() == -1


def test_file_picker_pick_compaction_level_returns_minus_one_below_l0_trigger():
    levels = [
        [
            {
                "file_id": 1,
                "smallest_key": b"k1",
                "largest_key": b"k1",
                "file_size": 100,
            },
            {
                "file_id": 2,
                "smallest_key": b"k2",
                "largest_key": b"k2",
                "file_size": 100,
            },
            {
                "file_id": 3,
                "smallest_key": b"k3",
                "largest_key": b"k3",
                "file_size": 100,
            },
        ],
        [],
    ]
    picker = FilePicker(
        levels=levels,
        max_levels=7,
        sstable_l0_compaction_trigger=settings.sstable_l0_compaction_trigger,
        sstable_l1_max_bytes=settings.sstable_l1_max_bytes,
        sstable_level_multiplier=settings.sstable_level_multiplier,
    )

    assert picker.pick_compaction_level() == -1


def test_file_picker_pick_compaction_level_returns_l1_on_size_overflow():
    levels = [
        [],
        [
            {
                "file_id": 10,
                "smallest_key": b"a",
                "largest_key": b"z",
                "file_size": SSTABLE_L1_MAX_BYTES + 1,
            }
        ],
        [],
    ]
    picker = FilePicker(
        levels=levels,
        max_levels=7,
        sstable_l0_compaction_trigger=settings.sstable_l0_compaction_trigger,
        sstable_l1_max_bytes=settings.sstable_l1_max_bytes,
        sstable_level_multiplier=settings.sstable_level_multiplier,
    )

    assert picker.pick_compaction_level() == 1


def test_file_picker_pick_compaction_files_empty_source_returns_empty_lists():
    levels = [[], [], []]
    picker = FilePicker(
        levels=levels,
        max_levels=7,
        sstable_l0_compaction_trigger=settings.sstable_l0_compaction_trigger,
        sstable_l1_max_bytes=settings.sstable_l1_max_bytes,
        sstable_level_multiplier=settings.sstable_level_multiplier,
    )

    source_metas, target_metas = picker.pick_compaction_files(1, 2)

    assert source_metas == []
    assert target_metas == []


def test_file_picker_pick_compaction_files_l0_empty_source_returns_empty_lists():
    levels = [[], []]
    picker = FilePicker(
        levels=levels,
        max_levels=7,
        sstable_l0_compaction_trigger=settings.sstable_l0_compaction_trigger,
        sstable_l1_max_bytes=settings.sstable_l1_max_bytes,
        sstable_level_multiplier=settings.sstable_level_multiplier,
    )

    source_metas, target_metas = picker.pick_compaction_files(0, 1)

    assert source_metas == []
    assert target_metas == []


def test_file_picker_pick_compaction_files_non_l0_empty_source_returns_empty_lists():
    levels = [
        [
            {
                "file_id": 1,
                "smallest_key": b"a",
                "largest_key": b"a",
                "file_size": 10,
            }
        ],
        [],
        [
            {
                "file_id": 2,
                "smallest_key": b"a",
                "largest_key": b"z",
                "file_size": 10,
            }
        ],
    ]
    picker = FilePicker(
        levels=levels,
        max_levels=7,
        sstable_l0_compaction_trigger=settings.sstable_l0_compaction_trigger,
        sstable_l1_max_bytes=settings.sstable_l1_max_bytes,
        sstable_level_multiplier=settings.sstable_level_multiplier,
    )

    source_metas, target_metas = picker.pick_compaction_files(1, 2)

    assert source_metas == []
    assert target_metas == []


def test_file_picker_pick_compaction_files_non_l0_selects_overlapping_targets():
    source_level = [
        {
            "file_id": 10,
            "smallest_key": b"k2",
            "largest_key": b"k5",
            "file_size": 10,
        },
        {
            "file_id": 11,
            "smallest_key": b"x1",
            "largest_key": b"x9",
            "file_size": 10,
        },
    ]
    target_level = [
        {
            "file_id": 20,
            "smallest_key": b"k0",
            "largest_key": b"k3",
            "file_size": 10,
        },
        {
            "file_id": 21,
            "smallest_key": b"k4",
            "largest_key": b"k8",
            "file_size": 10,
        },
        {
            "file_id": 22,
            "smallest_key": b"z0",
            "largest_key": b"z9",
            "file_size": 10,
        },
    ]

    levels = [[], source_level[:], target_level[:], []]
    picker = FilePicker(
        levels=levels,
        max_levels=7,
        sstable_l0_compaction_trigger=settings.sstable_l0_compaction_trigger,
        sstable_l1_max_bytes=settings.sstable_l1_max_bytes,
        sstable_level_multiplier=settings.sstable_level_multiplier,
    )

    source_metas, target_metas = picker.pick_compaction_files(1, 2)

    # For non-L0 levels only the first source file is selected.
    assert source_metas == [source_level[0]]
    # Both overlapping ranges are selected, non-overlap is excluded.
    assert target_metas == [target_level[0], target_level[1]]


def test_file_picker_pick_compaction_level_and_files_for_l0():
    level0 = [
        {
            "file_id": 1,
            "smallest_key": b"key1",
            "largest_key": b"key1",
            "file_size": 10,
        },
        {
            "file_id": 2,
            "smallest_key": b"key2",
            "largest_key": b"key2",
            "file_size": 10,
        },
        {
            "file_id": 3,
            "smallest_key": b"key3",
            "largest_key": b"key3",
            "file_size": 10,
        },
        {
            "file_id": 4,
            "smallest_key": b"key4",
            "largest_key": b"key4",
            "file_size": 10,
        },
    ]
    level1 = [
        {
            "file_id": 5,
            "smallest_key": b"key0",
            "largest_key": b"key9",
            "file_size": 10,
        }
    ]
    levels = [level0[:], level1[:], []]
    picker = FilePicker(
        levels=levels,
        max_levels=7,
        sstable_l0_compaction_trigger=settings.sstable_l0_compaction_trigger,
        sstable_l1_max_bytes=settings.sstable_l1_max_bytes,
        sstable_level_multiplier=settings.sstable_level_multiplier,
    )

    assert picker.pick_compaction_level() == 0

    source_metas, target_metas = picker.pick_compaction_files(0, 1)

    assert source_metas == level0
    assert target_metas == level1
