import pytest

from cailloudb.lsm.bloom_filter import BloomFilterBuilder, BloomFilterReader


def test_bloom_filter_build_and_read():
    keys = [b"apple", b"banana", b"cherry"]
    builder = BloomFilterBuilder()
    for key in keys:
        builder.add_key(key)

    reader = BloomFilterReader(builder.finalize())

    for key in keys:
        assert reader.may_find(key)


def test_bloom_filter_empty():
    builder = BloomFilterBuilder()
    reader = BloomFilterReader(builder.pack())

    assert not reader.may_find(b"apple")


def test_bloom_filter_finalize_resets_builder():
    builder = BloomFilterBuilder()
    builder.add_key(b"apple")
    reader = BloomFilterReader(builder.finalize())
    empty_reader = BloomFilterReader(builder.pack())

    assert reader.may_find(b"apple")
    assert not empty_reader.may_find(b"apple")


def test_bloom_filter_versioned_encoding():
    builder = BloomFilterBuilder()
    for key in (b"apple", b"banana", b"cherry"):
        builder.add_key(key)

    assert builder.pack() == bytes.fromhex("0880b942881150410601")


def test_bloom_filter_reads_version_1():
    reader = BloomFilterReader(bytes.fromhex("0880b942881150410601"))

    for key in (b"apple", b"banana", b"cherry"):
        assert reader.may_find(key)
    assert not reader.may_find(b"zebra")


@pytest.mark.parametrize("version", [0, 2, 255])
def test_bloom_filter_unknown_version_is_a_possible_match(version: int):
    reader = BloomFilterReader(bytes(8) + bytes((6, version)))

    assert reader.may_find(b"apple")


@pytest.mark.parametrize(
    "filter_data",
    [b"", b"\x01", b"\x06\x01", bytes(7) + b"\x06\x01"],
)
def test_bloom_filter_missing_or_truncated_is_a_possible_match(filter_data: bytes):
    assert BloomFilterReader(filter_data).may_find(b"apple")


@pytest.mark.parametrize("num_probes", [0, 31, 255])
def test_bloom_filter_invalid_probe_count_is_a_possible_match(num_probes: int):
    reader = BloomFilterReader(bytes(8) + bytes((num_probes, 1)))

    assert reader.may_find(b"apple")


@pytest.mark.parametrize("key_count", [0, 3, 20])
def test_bloom_filter_estimated_size_includes_trailer(key_count: int):
    builder = BloomFilterBuilder()
    for i in range(key_count):
        builder.add_key(f"key{i}".encode())

    assert builder.estimated_size() == len(builder.pack())
