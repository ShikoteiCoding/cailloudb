import os
import subprocess
import sys

import pytest

from cailloudb.lsm.bloom_filter import BloomFilterBuilder, BloomFilterReader


@pytest.mark.parametrize("bits_per_key", [1, 10, 64])
def test_bloom_filter_has_no_false_negatives(bits_per_key):
    keys = [bytes(range(n)) for n in range(9)] + [bytes(range(256))]
    keys += [f"key:{i}".encode() for i in range(1000)]
    builder = BloomFilterBuilder(bits_per_key)
    for key in keys:
        builder.add_key(key)

    expected_size = builder.estimated_size()
    data = builder.finish()
    reader = BloomFilterReader(data)

    assert len(data) == expected_size
    assert all(reader.may_find(key) for key in keys)


def test_bloom_filter_rejects_most_missing_keys():
    builder = BloomFilterBuilder()
    for i in range(10_000):
        builder.add_key(f"present:{i}".encode())
    reader = BloomFilterReader(builder.finish())

    false_positives = sum(
        reader.may_find(f"missing:{i}".encode()) for i in range(10_000)
    )

    assert false_positives < 200


@pytest.mark.parametrize("hash_seed", ["1", "2"])
def test_bloom_filter_can_be_read_in_another_process(tmp_path, hash_seed):
    builder = BloomFilterBuilder()
    builder.add_key(b"persisted")
    builder.add_key(bytes(range(256)))
    filter_path = tmp_path / "filter.bin"
    filter_path.write_bytes(builder.finish())

    code = """
import sys
from pathlib import Path
from cailloudb.lsm.bloom_filter import BloomFilterReader

reader = BloomFilterReader(Path(sys.argv[1]).read_bytes())
assert reader.may_find(b"persisted")
assert reader.may_find(bytes(range(256)))
"""
    subprocess.run(
        [sys.executable, "-c", code, str(filter_path)],
        env={**os.environ, "PYTHONHASHSEED": hash_seed},
        capture_output=True,
        text=True,
        check=True,
    )


def test_bloom_filter_finish_resets_builder():
    builder = BloomFilterBuilder()
    builder.add_key(b"first")
    first = BloomFilterReader(builder.finish())
    empty = BloomFilterReader(builder.write())
    builder.add_key(b"second")
    second = BloomFilterReader(builder.finish())

    assert not empty.may_find(b"first")
    assert second.may_find(b"second")
    assert first.may_find(b"first")
