"""
test_lsmtree focuses on testing internals of the lsm as it acts
as an orchestration layer between in-memory and disk storage.
"""

import time

import pytest

from cailloudb.custom_types import MemTableEntry
from cailloudb.lsm.lsmtree import LSMTree
from cailloudb.lsm.memtable import MemTable
from cailloudb.lsm.utils import build_internal_key
from cailloudb.settings import TOMBSTONE

__all__ = [
    "test_lsmtree_internals_iteratively_0_record_memtable",
    "test_lsmtree_internals_iteratively_1_records_memtables",
    "test_lsmtree_get_from_skiplist",
    "test_lsmtree_get_from_immutable_memtable",
    "test_lsmtree_get_level_gt_0_breaks_when_key_below_smallest",
    "test_lsmtree_get_from_sstable_because_of_spill",
    "test_lsmtree_get_from_sstable_or_skiplist",
    "test_lsm_delete_from_sstables_or_skiplist",
    "test_lsmtree_scan_includes_immutable_memtables",
    "test_lsmtree_scan_skips_file_when_before_start_key",
    "test_lsmtree_scan_level0_continues_on_end_bound",
    "test_lsmtree_scan_level_gt_0_breaks_on_end_bound",
    "test_lsmtree_scan_fixed_range",
    "test_lsmtree_scan_unbounded_range",
    "test_lsmtree_scan_max_seq",
    "test_lsmtree_scan_iterator_agg_break",
    "test_lsmtree_scan_future_seq_continue",
    "test_rotate_clears_wal_before_flush",
    "test_lsmtree_flush_empty_flush",
    "test_lsmtree_flush_worker_returns_when_queue_is_none",
    "test_lsmtree_flush_worker_exception_path",
    "test_lsmtree_start_compaction_worker_noop_when_disabled",
    "test_lsmtree_compaction_worker_natural_background_flow_default_runtime",
    "test_lsmtree_sync_compact_exits_when_picker_finds_no_level",
    "test_lsmtree_sync_compact_exits_when_picker_returns_no_files",
    "test_lsmtree_sync_compact_uses_picker_selected_files",
    "test_lsmtree_sync_compact_replaces_levels_and_map",
    "test_lsmtree_sync_compact_then_scan",
    "test_lsmtree_sync_compact_then_get",
]


def _wait_for_condition(predicate, timeout_s: float = 2.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("Timed out waiting for condition")


@pytest.mark.asyncio
async def test_lsmtree_internals_iteratively_0_record_memtable():
    lsmtree = LSMTree(10)  # should be full for each record

    await lsmtree.put(b"key1", 0, b"val1")
    _wait_for_condition(
        lambda: (
            lsmtree._flush_queue.unfinished_tasks == 0
            and not lsmtree._immutable_memtables
        )
    )
    assert len(lsmtree._memtable) == 0
    assert len(list(lsmtree._sstables_map.values())) == 1

    await lsmtree.put(b"key2", 1, b"val2")
    _wait_for_condition(
        lambda: (
            lsmtree._flush_queue.unfinished_tasks == 0
            and not lsmtree._immutable_memtables
        )
    )
    assert len(lsmtree._memtable) == 0
    assert len(list(lsmtree._sstables_map.values())) == 2
    assert len(list(lsmtree._sstables_map.values())[0].index_keys) == 1

    await lsmtree.put(b"key3", 2, b"val3")
    _wait_for_condition(
        lambda: (
            lsmtree._flush_queue.unfinished_tasks == 0
            and not lsmtree._immutable_memtables
        )
    )
    assert len(lsmtree._memtable) == 0
    assert len(list(lsmtree._sstables_map.values())) == 3
    assert len(list(lsmtree._sstables_map.values())[0].index_keys) == 1


@pytest.mark.asyncio
async def test_lsmtree_internals_iteratively_1_records_memtables():
    lsmtree = LSMTree(30)  # should hold only 1 record

    await lsmtree.put(b"key1", 0, b"val1")
    assert len(lsmtree._memtable) == 1
    assert len(list(lsmtree._sstables_map.values())) == 0

    await lsmtree.put(b"key2", 1, b"val2")
    _wait_for_condition(
        lambda: (
            lsmtree._flush_queue.unfinished_tasks == 0
            and not lsmtree._immutable_memtables
        )
    )
    assert len(lsmtree._memtable) == 0
    assert len(list(lsmtree._sstables_map.values())) == 1
    assert len(list(lsmtree._sstables_map.values())[0].index_keys) == 1

    await lsmtree.put(b"key3", 2, b"val3")
    assert len(lsmtree._memtable) == 1
    assert len(list(lsmtree._sstables_map.values())) == 1
    assert len(list(lsmtree._sstables_map.values())[0].index_keys) == 1


@pytest.mark.asyncio
async def test_lsmtree_get_from_skiplist():
    lsmtree = LSMTree()

    await lsmtree.put(b"key1", 0, b"val1")

    value = lsmtree.get(b"key1", 0)

    assert value == b"val1"


def test_lsmtree_get_from_immutable_memtable():
    lsmtree = LSMTree()

    older_imm = MemTable(max_bytes_size=128)
    older_imm.insert(b"key1", 0, b"val1")

    newer_imm = MemTable(max_bytes_size=128)
    newer_imm.insert(b"key1", 1, TOMBSTONE)

    # XXX: Remove this when flush is background
    lsmtree._immutable_memtables = [older_imm, newer_imm]

    assert lsmtree.get(b"key1", 1) is None
    assert lsmtree.get(b"key1", 0) == b"val1"


def test_lsmtree_get_level_gt_0_breaks_when_key_below_smallest():
    lsmtree = LSMTree()

    class NeverCalledSSTable:
        def get(self, _key: bytes, _seq_num: int):
            raise AssertionError("SSTable.get should not be called when key < smallest")

    meta = {
        "file_id": 1,
        "smallest_key": build_internal_key(b"key5", 0, is_delete=False),
        "largest_key": build_internal_key(b"key9", 0, is_delete=False),
        "file_size": 100,
    }

    lsmtree._levels[1] = [meta]  # type: ignore
    lsmtree._sstables_map[1] = NeverCalledSSTable()  # type: ignore

    assert lsmtree.get(b"key1", 10) is None


@pytest.mark.asyncio
async def test_lsmtree_get_from_sstable_because_of_spill():
    lsmtree = LSMTree(10)  # should be full for each record

    await lsmtree.put(b"key1", 0, b"val1")
    await lsmtree.put(b"key2", 1, b"val2")
    await lsmtree.put(b"key3", 2, b"val3")
    _wait_for_condition(
        lambda: (
            lsmtree._flush_queue.unfinished_tasks == 0
            and not lsmtree._immutable_memtables
        )
    )

    # Should get it from "oldest" sstable
    assert lsmtree.get(b"key1", 2) == b"val1"
    assert lsmtree._memtable.get(b"key1", 2) is None
    assert list(lsmtree._sstables_map.values())[0].get(b"key1", 2) == MemTableEntry(
        key=b"key1", seq_num=0, value=b"val1"
    )

    # Should get it from "middle" sstable
    assert lsmtree.get(b"key2", 2) == b"val2"
    assert lsmtree._memtable.get(b"key2", 2) is None
    assert list(lsmtree._sstables_map.values())[1].get(b"key2", 2) == MemTableEntry(
        key=b"key2", seq_num=1, value=b"val2"
    )

    # Should get it from "recent" sstable
    assert lsmtree.get(b"key3", 2) == b"val3"
    assert list(lsmtree._sstables_map.values())[2].get(b"key3", 2) == MemTableEntry(
        key=b"key3", seq_num=2, value=b"val3"
    )


@pytest.mark.asyncio
async def test_lsmtree_get_from_sstable_or_skiplist():
    lsmtree = LSMTree(30)  # should hold only 1 record

    await lsmtree.put(b"key1", 0, b"val1")
    await lsmtree.put(b"key2", 1, b"val2")
    await lsmtree.put(b"key3", 2, b"val3")
    _wait_for_condition(
        lambda: (
            lsmtree._flush_queue.unfinished_tasks == 0
            and not lsmtree._immutable_memtables
        )
    )

    # Each insertion immediately fills the active skiplist
    # SSTable are then flushed
    # and a fresh memtable is created
    assert lsmtree._memtable.bytes_size > 0
    assert len(list(lsmtree._sstables_map.values())) == 1

    # Should get it from "oldest" sstable
    assert lsmtree.get(b"key1", 2) == b"val1"
    assert lsmtree._memtable.get(b"key1", 2) is None
    assert list(lsmtree._sstables_map.values())[0].get(b"key1", 2) == MemTableEntry(
        key=b"key1", seq_num=0, value=b"val1"
    )

    # Should get it from "recent" sstable
    assert lsmtree.get(b"key2", 2) == b"val2"
    assert lsmtree._memtable.get(b"key2", 2) is None
    assert list(lsmtree._sstables_map.values())[0].get(b"key2", 2) == MemTableEntry(
        key=b"key2", seq_num=1, value=b"val2"
    )

    # Should get it from skiplist
    assert lsmtree.get(b"key3", 2) == b"val3"
    assert lsmtree._memtable.get(b"key3", 2) == MemTableEntry(
        key=b"key3", seq_num=2, value=b"val3"
    )


@pytest.mark.asyncio
async def test_lsm_delete_from_sstables_or_skiplist():
    lsmtree = LSMTree(30)  # should hold only 1 record

    await lsmtree.delete(b"key1", 0)
    await lsmtree.delete(b"key2", 1)
    await lsmtree.delete(b"key3", 2)

    assert lsmtree.get(b"key1", 2) is None
    assert lsmtree.get(b"key2", 2) is None
    assert lsmtree.get(b"key3", 2) is None


def test_lsmtree_scan_includes_immutable_memtables():
    lsmtree = LSMTree()

    imm = MemTable(max_bytes_size=128)
    imm.insert(b"key_imm", 1, b"val_imm")

    # Keep active memtable empty so values come from immutable memtables only.
    lsmtree._immutable_memtables = [imm]

    result = list(lsmtree.scan(start_key=None, end_key=None, seq_num=10))

    assert result == [(b"key_imm", b"val_imm")]


def test_lsmtree_scan_skips_file_when_before_start_key():
    lsmtree = LSMTree()

    class CountingSSTable:
        def __init__(self, entries):
            self.entries = entries
            self.calls = 0

        def scan(self, _start_key, _end_key, _seq_num):
            self.calls += 1
            return iter(self.entries)

    meta1 = {
        "file_id": 1,
        "smallest_key": build_internal_key(b"key1", 0, is_delete=False),
        "largest_key": build_internal_key(b"key2", 0, is_delete=False),
        "file_size": 100,
    }
    meta2 = {
        "file_id": 2,
        "smallest_key": build_internal_key(b"key5", 0, is_delete=False),
        "largest_key": build_internal_key(b"key8", 0, is_delete=False),
        "file_size": 100,
    }

    sst1 = CountingSSTable(entries=[MemTableEntry(key=b"key1", seq_num=0, value=b"v1")])
    sst2 = CountingSSTable(entries=[MemTableEntry(key=b"key6", seq_num=1, value=b"v6")])

    lsmtree._levels[1] = [meta1, meta2]  # type: ignore
    lsmtree._sstables_map[1] = sst1  # type: ignore
    lsmtree._sstables_map[2] = sst2  # type: ignore

    result = list(lsmtree.scan(start_key=b"key5", end_key=None, seq_num=10))

    assert sst1.calls == 0
    assert sst2.calls == 1
    assert result == [(b"key6", b"v6")]


def test_lsmtree_scan_level0_continues_on_end_bound():
    lsmtree = LSMTree()

    class CountingSSTable:
        def __init__(self, entries):
            self.entries = entries
            self.calls = 0

        def scan(self, _start_key, _end_key, _seq_num):
            self.calls += 1
            return iter(self.entries)

    # In level 0, `file_smallest_key >= end_key` should continue, not break.
    meta_skip = {
        "file_id": 1,
        "smallest_key": build_internal_key(b"key9", 0, is_delete=False),
        "largest_key": build_internal_key(b"key9", 0, is_delete=False),
        "file_size": 100,
    }
    meta_in_range = {
        "file_id": 2,
        "smallest_key": build_internal_key(b"key1", 0, is_delete=False),
        "largest_key": build_internal_key(b"key3", 0, is_delete=False),
        "file_size": 100,
    }

    sst_skip = CountingSSTable(
        entries=[MemTableEntry(key=b"key9", seq_num=0, value=b"v9")]
    )
    sst_in_range = CountingSSTable(
        entries=[MemTableEntry(key=b"key2", seq_num=1, value=b"v2")]
    )

    lsmtree._levels[0] = [meta_skip, meta_in_range]  # type: ignore
    lsmtree._sstables_map[1] = sst_skip  # type: ignore
    lsmtree._sstables_map[2] = sst_in_range  # type: ignore

    result = list(lsmtree.scan(start_key=None, end_key=b"key5", seq_num=10))

    assert sst_skip.calls == 0
    assert sst_in_range.calls == 1
    assert result == [(b"key2", b"v2")]


def test_lsmtree_scan_level_gt_0_breaks_on_end_bound():
    lsmtree = LSMTree()

    class CountingSSTable:
        def __init__(self, entries):
            self.entries = entries
            self.calls = 0

        def scan(self, _start_key, _end_key, _seq_num):
            self.calls += 1
            return iter(self.entries)

    meta_break = {
        "file_id": 1,
        "smallest_key": build_internal_key(b"key9", 0, is_delete=False),
        "largest_key": build_internal_key(b"key9", 0, is_delete=False),
        "file_size": 100,
    }
    meta_never_reached = {
        "file_id": 2,
        "smallest_key": build_internal_key(b"key1", 0, is_delete=False),
        "largest_key": build_internal_key(b"key3", 0, is_delete=False),
        "file_size": 100,
    }

    sst_break = CountingSSTable(
        entries=[MemTableEntry(key=b"key9", seq_num=0, value=b"v9")]
    )
    sst_never_reached = CountingSSTable(
        entries=[MemTableEntry(key=b"key2", seq_num=1, value=b"v2")]
    )

    lsmtree._levels[1] = [meta_break, meta_never_reached]  # type: ignore
    lsmtree._sstables_map[1] = sst_break  # type: ignore
    lsmtree._sstables_map[2] = sst_never_reached  # type: ignore

    result = list(lsmtree.scan(start_key=None, end_key=b"key5", seq_num=10))

    assert sst_break.calls == 0
    assert sst_never_reached.calls == 0
    assert result == []


@pytest.mark.asyncio
async def test_lsmtree_scan_fixed_range():
    lsmtree = LSMTree(30)  # should hold only 1 record

    await lsmtree.put(b"key1", 0, b"val1")
    await lsmtree.put(b"key2", 1, b"val2")
    await lsmtree.put(b"key3", 2, b"val3")

    # Test inclusive
    result = [item for item in lsmtree.scan(b"key", b"key4", 2)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
        (b"key3", b"val3"),
    ]

    # Test excluding start_key
    result = [item for item in lsmtree.scan(b"key2", b"key4", 2)]
    assert result == [
        (b"key2", b"val2"),
        (b"key3", b"val3"),
    ]

    # Test excluding end_key
    result = [item for item in lsmtree.scan(b"key1", b"key3", 2)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
    ]

    # Test excluding both
    result = [item for item in lsmtree.scan(b"key2", b"key3", 2)]
    assert result == [
        (b"key2", b"val2"),
    ]


@pytest.mark.asyncio
async def test_lsmtree_scan_unbounded_range():
    lsmtree = LSMTree(30)  # should hold only 1 record

    await lsmtree.put(b"key1", 0, b"val1")
    await lsmtree.put(b"key2", 1, b"val2")
    await lsmtree.put(b"key3", 2, b"val3")

    # Test unbounded end_key
    result = [item for item in lsmtree.scan(b"key1", None, 2)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
        (b"key3", b"val3"),
    ]

    # Test unbounded start_key
    result = [item for item in lsmtree.scan(None, b"key4", 2)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
        (b"key3", b"val3"),
    ]

    # Test unbounded both
    result = [item for item in lsmtree.scan(None, None, 2)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
        (b"key3", b"val3"),
    ]


@pytest.mark.asyncio
async def test_lsmtree_scan_max_seq():
    lsmtree = LSMTree(30)  # should hold only 1 record

    await lsmtree.put(b"key1", 0, b"val1")
    await lsmtree.put(b"key2", 1, b"val2")
    await lsmtree.put(b"key3", 2, b"val3")

    # Test fixed range
    result = [item for item in lsmtree.scan(b"key1", b"key4", 1)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
    ]

    # Test unbounded
    result = [item for item in lsmtree.scan(None, None, 1)]
    assert result == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
    ]


def test_lsmtree_scan_iterator_agg_break(monkeypatch: pytest.MonkeyPatch):
    lsmtree = LSMTree()

    # Mock iterator_agg internal function from scan()
    mock_iterator = iter(
        [
            {"key": b"key1", "seq_num": 1, "value": b"val1"},
            {
                "key": b"key3",
                "seq_num": 2,
                "value": b"val3",
            },  # >= end_key, triggers `break`
            {
                "key": b"key4",
                "seq_num": 3,
                "value": b"val4",
            },  # Should never be processed
        ]
    )

    monkeypatch.setattr(
        lsmtree._memtable,
        "scan",
        lambda _start_key, _end_key, _seq_num: mock_iterator,
    )
    assert len(list(lsmtree._sstables_map.values())) == 0

    # end_key=b"key2" means b"key3" will trigger the break condition
    result = list(lsmtree.scan(start_key=b"key0", end_key=b"key2", seq_num=5))

    # Only b"key1" should be yielded
    assert result == [(b"key1", b"val1")]


def test_lsmtree_scan_future_seq_continue(monkeypatch: pytest.MonkeyPatch):
    lsmtree = LSMTree()

    # Mock iterator_agg internal function from scan()
    mock_iterator = iter(
        [
            {
                "key": b"key1",
                "seq_num": 10,
                "value": b"future_val",
            },  # > seq_num, triggers `continue`
            {"key": b"key1", "seq_num": 5, "value": b"past_val"},  # Valid version
        ]
    )
    monkeypatch.setattr(
        lsmtree._memtable,
        "scan",
        lambda _start_key, _end_key, _seq_num: mock_iterator,
    )
    lsmtree._sstables_map = {}

    # seq_num=7 means seq_num=10 is from the future and should be skipped
    result = list(lsmtree.scan(start_key=None, end_key=None, seq_num=7))

    # The future_val should be skipped, yielding only past_val
    assert result == [(b"key1", b"past_val")]


@pytest.mark.asyncio
async def test_rotate_clears_wal_before_flush(tmp_path):
    lsmtree = LSMTree(10, wal_path=tmp_path / "wal")

    await lsmtree.put(b"key1", 0, b"val1")
    _wait_for_condition(
        lambda: (
            lsmtree._flush_queue.unfinished_tasks == 0
            and not lsmtree._immutable_memtables
        )
    )

    sealed = tmp_path / "wal" / "wal_000001.log"
    assert sealed.stat().st_size > 0
    assert lsmtree._wal is not None
    assert lsmtree._wal._path.name == "wal_000002.log"
    assert lsmtree._wal._path.stat().st_size == 0
    assert len(lsmtree._sstables_map) == 1


def test_lsmtree_flush_empty_flush(monkeypatch: pytest.MonkeyPatch):
    lsmtree = LSMTree()

    _wait_for_condition(
        lambda: (
            lsmtree._flush_queue.unfinished_tasks == 0
            and not lsmtree._immutable_memtables
        )
    )

    assert len(list(lsmtree._sstables_map.values())) == 0


def test_lsmtree_flush_worker_returns_when_queue_is_none():
    lsmtree = LSMTree()
    lsmtree._flush_queue = None  # type: ignore[assignment]

    lsmtree._flush_worker()

    assert lsmtree._sstables_map == {}


def test_lsmtree_flush_worker_exception_path():
    lsmtree = LSMTree()

    class StubQueue:
        def __init__(self):
            self.calls = 0

        def get(self):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("boom")
            raise KeyboardInterrupt()

        def task_done(self):
            pass

    stub_queue = StubQueue()
    lsmtree._flush_queue = stub_queue  # type: ignore

    with pytest.raises(KeyboardInterrupt):
        lsmtree._flush_worker()

    assert stub_queue.calls == 2


def test_lsmtree_start_compaction_worker_noop_when_disabled():
    lsmtree = LSMTree(compaction_enabled=False)

    lsmtree._start_compaction_worker()

    assert lsmtree._compaction_thread is None


@pytest.mark.asyncio
async def test_lsmtree_compaction_worker_natural_background_flow_default_runtime():
    lsmtree = LSMTree(
        memtable_size=10,
        sstable_size=10,
        compaction_enabled=True,
        compaction_interval_s=1,
    )

    assert lsmtree._compaction_thread is not None
    assert lsmtree._compaction_thread.is_alive()

    await lsmtree.put(b"key1", 0, b"val1")
    await lsmtree.put(b"key2", 1, b"val2")
    await lsmtree.put(b"key3", 2, b"val3")
    await lsmtree.put(b"key4", 3, b"val4")

    _wait_for_condition(
        lambda: (
            lsmtree._flush_queue.unfinished_tasks == 0
            and not lsmtree._immutable_memtables
        )
    )

    _wait_for_condition(
        lambda: len(lsmtree._levels[1]) == 4,
        timeout_s=2.0,
    )

    assert len(lsmtree._levels[1]) == 4


@pytest.mark.asyncio
async def test_lsmtree_sync_compact_exits_when_picker_finds_no_level():
    lsmtree = LSMTree(
        memtable_size=10,  # should hold only 1 record
        sstable_size=10,  # should hold only 1 record
    )

    # 3 files in L0 is below default compaction trigger (4).
    await lsmtree.put(b"key1", 0, b"val1")
    await lsmtree.put(b"key2", 1, b"val2")
    await lsmtree.put(b"key3", 2, b"val3")
    _wait_for_condition(
        lambda: (
            lsmtree._flush_queue.unfinished_tasks == 0
            and not lsmtree._immutable_memtables
        )
    )

    assert len(lsmtree._levels[0]) == 3
    old_level0_file_ids = [meta["file_id"] for meta in lsmtree._levels[0]]
    old_map_file_ids = sorted(lsmtree._sstables_map.keys())
    old_next_file_id = lsmtree._next_file_id

    lsmtree._commit_compaction_result(lsmtree._pick_and_compact())

    assert [meta["file_id"] for meta in lsmtree._levels[0]] == old_level0_file_ids
    assert sorted(lsmtree._sstables_map.keys()) == old_map_file_ids
    assert lsmtree._next_file_id == old_next_file_id


def test_lsmtree_sync_compact_exits_when_picker_returns_no_files(
    monkeypatch: pytest.MonkeyPatch,
):
    lsmtree = LSMTree()

    # Ensure we pass the `source_level == -1` guard and hit `if not input_metas: return`.
    monkeypatch.setattr(lsmtree._file_picker, "pick_compaction_level", lambda: 0)
    monkeypatch.setattr(
        lsmtree._file_picker,
        "pick_compaction_files",
        lambda _source_level, _target_level: ([], []),
    )

    def _should_not_compact(*_args, **_kwargs):
        raise AssertionError(
            "Compactor should not run when no input metas are selected"
        )

    monkeypatch.setattr(lsmtree._compactor, "compact", _should_not_compact)

    old_levels = [list(level) for level in lsmtree._levels]
    old_map = dict(lsmtree._sstables_map)
    old_next_file_id = lsmtree._next_file_id

    lsmtree._pick_and_compact()

    assert lsmtree._levels == old_levels
    assert lsmtree._sstables_map == old_map
    assert lsmtree._next_file_id == old_next_file_id


@pytest.mark.asyncio
async def test_lsmtree_sync_compact_uses_picker_selected_files():
    lsmtree = LSMTree(
        memtable_size=10,  # should hold only 1 record
        sstable_size=10,  # should hold only 1 record
    )

    # 4 files in L0 should trigger compaction selection for level 0.
    await lsmtree.put(b"key1", 0, b"val1")
    await lsmtree.put(b"key2", 1, b"val2")
    await lsmtree.put(b"key3", 2, b"val3")
    await lsmtree.put(b"key4", 3, b"val4")
    _wait_for_condition(
        lambda: (
            lsmtree._flush_queue.unfinished_tasks == 0
            and not lsmtree._immutable_memtables
        )
    )
    old_level0_file_ids = {meta["file_id"] for meta in lsmtree._levels[0]}

    lsmtree._commit_compaction_result(lsmtree._pick_and_compact())

    assert lsmtree._levels[0] == []
    assert len(lsmtree._levels[1]) == 4
    new_level1_file_ids = {meta["file_id"] for meta in lsmtree._levels[1]}
    assert old_level0_file_ids.isdisjoint(new_level1_file_ids)
    assert new_level1_file_ids.issubset(set(lsmtree._sstables_map.keys()))


def test_lsmtree_sync_compact_replaces_levels_and_map(
    monkeypatch: pytest.MonkeyPatch,
):
    lsmtree = LSMTree()

    source_meta = {
        "file_id": 1,
        "smallest_key": b"key1",
        "largest_key": b"key3",
        "file_size": 100,
    }
    target_meta = {
        "file_id": 2,
        "smallest_key": b"k2",
        "largest_key": b"k4",
        "file_size": 100,
    }
    lsmtree._levels[0] = [source_meta]  # type: ignore
    lsmtree._levels[1] = [target_meta]  # type: ignore

    source_sstable = object()
    target_sstable = object()
    lsmtree._sstables_map = {  # type: ignore
        1: source_sstable,
        2: target_sstable,
    }
    lsmtree._next_file_id = 10

    generated_meta = {
        "file_id": 10,
        "smallest_key": b"k1",
        "largest_key": b"k4",
        "file_size": 120,
    }

    class MockSSTable:
        def __init__(self, file_id: int):
            self.file_id = file_id

    generated_sstable = MockSSTable(file_id=10)

    def mock_compact(sstables_to_merge, file_id_generator, level_sstable_size):
        assert sstables_to_merge == [source_sstable, target_sstable]
        assert file_id_generator() == 10
        assert level_sstable_size == lsmtree.sstable_size
        return [(generated_sstable, generated_meta)]

    monkeypatch.setattr(lsmtree._file_picker, "pick_compaction_level", lambda: 0)
    monkeypatch.setattr(lsmtree._compactor, "compact", mock_compact)

    lsmtree._commit_compaction_result(lsmtree._pick_and_compact())

    assert lsmtree._levels[0] == []
    assert lsmtree._levels[1] == [generated_meta]
    assert lsmtree._sstables_map[10] == generated_sstable
    assert lsmtree._sstables_map[1] == source_sstable
    assert lsmtree._sstables_map[2] == target_sstable
    assert lsmtree._next_file_id == 11


@pytest.mark.asyncio
async def test_lsmtree_sync_compact_then_scan():
    lsmtree = LSMTree(
        memtable_size=10,  # should hold only 1 record
        sstable_size=10,  # should hold only 1 record
    )

    await lsmtree.put(b"key1", 0, b"val1")
    await lsmtree.put(b"key2", 1, b"val2")
    await lsmtree.put(b"key3", 2, b"val3")
    await lsmtree.put(b"key4", 3, b"val4")
    _wait_for_condition(
        lambda: (
            lsmtree._flush_queue.unfinished_tasks == 0
            and not lsmtree._immutable_memtables
        )
    )

    assert len(lsmtree._levels[0]) == 4
    assert len(lsmtree._levels[1]) == 0

    old_level0_file_ids = {meta["file_id"] for meta in lsmtree._levels[0]}

    lsmtree._commit_compaction_result(lsmtree._pick_and_compact())

    assert lsmtree._levels[0] == []
    assert len(lsmtree._levels[1]) == 4

    new_level1_file_ids = {meta["file_id"] for meta in lsmtree._levels[1]}
    assert old_level0_file_ids.isdisjoint(new_level1_file_ids)

    assert new_level1_file_ids.issubset(set(lsmtree._sstables_map.keys()))

    assert list(lsmtree.scan(None, None, 2)) == [
        (b"key1", b"val1"),
        (b"key2", b"val2"),
        (b"key3", b"val3"),
    ]


@pytest.mark.asyncio
async def test_lsmtree_sync_compact_then_get():
    lsmtree = LSMTree(
        memtable_size=10,  # should hold only 1 record
        sstable_size=10,  # should hold only 1 record
    )

    await lsmtree.put(b"key1", 0, b"val1")
    await lsmtree.put(b"key2", 1, b"val2")
    await lsmtree.put(b"key3", 2, b"val3")
    await lsmtree.put(b"key4", 3, b"val4")
    _wait_for_condition(
        lambda: (
            lsmtree._flush_queue.unfinished_tasks == 0
            and not lsmtree._immutable_memtables
        )
    )

    assert len(lsmtree._levels[0]) == 4
    assert len(lsmtree._levels[1]) == 0

    lsmtree._commit_compaction_result(lsmtree._pick_and_compact())

    assert lsmtree._levels[0] == []
    assert len(lsmtree._levels[1]) == 4

    assert lsmtree.get(b"key1", 2) == b"val1"
    assert lsmtree.get(b"key2", 2) == b"val2"
    assert lsmtree.get(b"key3", 2) == b"val3"
    assert lsmtree.get(b"key3", 1) is None
