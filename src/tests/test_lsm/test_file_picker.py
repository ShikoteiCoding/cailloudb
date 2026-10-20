from cailloudb.lsm.filepicker import FilePicker

__all__ = [
    "test_file_picker_pick_compaction_level_returns_none_below_l0_trigger",
    "test_file_picker_pick_compaction_files_empty_source_returns_empty_lists",
    "test_file_picker_pick_compaction_level_and_files_for_l0",
]


def test_file_picker_pick_compaction_level_returns_none_below_l0_trigger():
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
    picker = FilePicker(levels=levels, max_levels=7)

    assert picker.pick_compaction_level() is None


def test_file_picker_pick_compaction_files_empty_source_returns_empty_lists():
    levels = [[], [], []]
    picker = FilePicker(levels=levels, max_levels=7)

    source_metas, target_metas = picker.pick_compaction_files(1, 2)

    assert source_metas == []
    assert target_metas == []


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
    picker = FilePicker(levels=levels, max_levels=7)

    assert picker.pick_compaction_level() == 0

    source_metas, target_metas = picker.pick_compaction_files(0, 1)

    assert source_metas == level0
    assert target_metas == level1
