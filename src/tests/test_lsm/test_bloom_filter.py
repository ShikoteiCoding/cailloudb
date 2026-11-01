from cailloudb.lsm.bloom_filter import BloomFilterBuilder, BloomFilterReader


def test_bloom_filter_build_and_read():
    keys = [b"apple", b"banana", b"cherry"]
    builder = BloomFilterBuilder()
    for key in keys:
        builder.add_key(key)

    reader = BloomFilterReader(builder.persist())

    for key in keys:
        assert reader.may_find(key)


def test_bloom_filter_empty():
    builder = BloomFilterBuilder()
    reader = BloomFilterReader(builder.write())

    assert not reader.may_find(b"apple")


def test_bloom_filter_persist_resets_builder():
    builder = BloomFilterBuilder()
    builder.add_key(b"apple")
    reader = BloomFilterReader(builder.persist())
    empty_reader = BloomFilterReader(builder.write())

    assert reader.may_find(b"apple")
    assert not empty_reader.may_find(b"apple")
