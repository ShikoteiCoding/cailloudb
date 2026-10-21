import pytest

from cailloudb.lsm.compactor import Compactor
from cailloudb.lsm.sstable import InMemorySSTable
from cailloudb.lsm.utils import build_internal_key

__all__ = [
    "test_compactor_deduplicate_iterator_keeps_latest_key_version",
    "test_compactor_deduplicate_iterator_keeps_first_tombstone",
]


def test_compactor_deduplicate_iterator_keeps_latest_key_version():
    compactor = Compactor()

    latest_a = (build_internal_key(b"a", 7, False), b"new-a")
    old_a = (build_internal_key(b"a", 3, False), b"old-a")
    latest_b = (build_internal_key(b"b", 9, False), b"new-b")
    old_b = (build_internal_key(b"b", 1, False), b"old-b")

    # Stream is globally sorted by internal key:
    # user_key ASC, seq_num DESC (because of inverted sequence in key encoding).
    merged_stream = iter([latest_a, old_a, latest_b, old_b])

    output = list(compactor._deduplicate_iterator(merged_stream))

    assert output == [latest_a, latest_b]


def test_compactor_deduplicate_iterator_keeps_first_tombstone():
    compactor = Compactor()

    tombstone_first = (build_internal_key(b"k", 5, True), b"")
    older_put = (build_internal_key(b"k", 2, False), b"value")

    output = list(compactor._deduplicate_iterator(iter([tombstone_first, older_put])))

    # Current compaction policy keeps newest version, even if tombstone.
    assert output == [tombstone_first]
