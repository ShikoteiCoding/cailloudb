from math import log
from zlib import crc32

from settings import (
    BLOOM_FILTER_VERSION,
    DEFAULT_BITS_PER_KEY,
    MASK32,
    MAX_PROBES,
    MIN_FILTER_BITS,
)

"""
Bloom filter (full filter, one per SSTable)
    [bit array][num_probes: 1 byte][version: 1 byte]

Hash each key once -> size the array -> set k bits per key

Reads use the same hash and delta to check those same k positions

For n keys, the target array size is m = n * bits_per_key bits.

More bits per key reduce false positives, so a positive result must still be checked against the SSTable

The filter is immutable once finished
After compaction (or any new SSTable) the builder is reset and a new array is allocated
"""


def _num_probes_for(bits_per_key: int) -> int:
    probes = int(bits_per_key * log(2))
    return max(1, min(MAX_PROBES, probes))


def _probe(h: int, bits: int, num_probes: int):
    # bit rotation to generate a second hash, rotation guarante to be non-zero
    delta = ((h >> 17) | (h << 15) | 1) & MASK32
    for _ in range(num_probes):
        yield h % bits
        h = (h + delta) & MASK32


class BloomFilterBuilder:
    """
    SSTable Bloom Filter Builder
    """

    #: Target bits per inserted key
    bits_per_key: int

    #: Hash probes per key (k)
    num_probes: int

    #: 32-bit hashes collected for the current SSTable
    hashes: list[int]

    def __init__(self, bits_per_key: int = DEFAULT_BITS_PER_KEY):
        self.bits_per_key = bits_per_key
        self.num_probes = _num_probes_for(bits_per_key)
        self.hashes: list[int] = []

    def add_key(self, key: bytes) -> None:
        """
        Record a key for the current SSTable's filter
        """
        # Collect hashes first, the array size depends on the final key count
        self.hashes.append(crc32(key))

    def estimated_size(self) -> int:
        """
        Return the encoded filter size, including probe count and version
        """
        total_keys = len(self.hashes)
        # m = n * bits_per_key, with a minimum size for small tables
        bits = max(MIN_FILTER_BITS, total_keys * self.bits_per_key)
        # a whole number of bytes, plus probe count and version
        return (bits + 7) // 8 + 2

    def pack(self) -> bytes:
        """
        Pack the current set of keys into an immutable filter

        Encoding:
            [bit array][num_probes][version]
        """
        nbytes = self.estimated_size() - 2
        bits = nbytes * 8
        array = bytearray(nbytes)

        # Set the same k positions that the reader will test for each key.
        for h in self.hashes:
            for bitpos in _probe(h, bits, self.num_probes):
                array[bitpos // 8] |= 1 << (bitpos % 8)

        array.extend((self.num_probes, BLOOM_FILTER_VERSION))
        return bytes(array)

    def finalize(self) -> bytes:
        """
        Return the finalized filter and reset for the next SSTable

        After compaction (or any new file) the key set changes, so bit array must be rebuilt from scratch
        """
        filter_data = self.pack()
        self.hashes = []
        return filter_data


class BloomFilterReader:
    """
    SSTable Bloom Filter Reader

    The filter is immutable
    Missing, too-short, or unsupported filter data is treated as a possible match
    """

    #: Packed filter blob
    data: bytes

    #: Bit-array length in bits
    bits: int

    #: Hash probes per key (k)
    num_probes: int

    def __init__(self, filter_data: bytes):
        self.data = filter_data
        self.bits = 0
        self.num_probes = 0

        if len(filter_data) < MIN_FILTER_BITS // 8 + 2:
            return
        if filter_data[-1] != BLOOM_FILTER_VERSION:
            return

        self.num_probes = filter_data[-2]
        self.bits = (len(filter_data) - 2) * 8

    def may_find(self, key: bytes) -> bool:
        """
        Return True if ``key`` may be present in the SSTable
        False means the key is missing
        """

        if self.bits == 0:
            return True

        if not 1 <= self.num_probes <= MAX_PROBES:
            return True

        array = self.data
        h = crc32(key)
        # One zero bit proves absence
        for bitpos in _probe(h, self.bits, self.num_probes):
            if (array[bitpos // 8] & (1 << (bitpos % 8))) == 0:
                return False
        return True
