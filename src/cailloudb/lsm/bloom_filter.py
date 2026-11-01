from math import log
from zlib import crc32

from settings import (
    _DEFAULT_BITS_PER_KEY,
    _MASK32,
    _MAX_PROBES,
    _MIN_FILTER_BITS,
)

"""
Bloom filter (full filter, one per SSTable)
    [bit array][num_probes: 1 byte]

Hash each key once -> size the array -> set k bits per key

Reads use the same hash and delta to check those same k positions

For n keys, the target array size is m = n * bits_per_key bits.

More bits per key reduce false positives, so a positive result must still be checked against the SSTable

The filter is immutable once finished
After compaction (or any new SSTable) the builder is reset and a new array is allocated
"""


def _num_probes_for(bits_per_key: int) -> int:
    probes = int(bits_per_key * log(2))
    return max(1, min(_MAX_PROBES, probes))


def _probe(h: int, bits: int, num_probes: int):
    # bit rotation to generate a second hash, rotation guarante to be non-zero
    delta = ((h >> 17) | (h << 15) | 1) & _MASK32
    for _ in range(num_probes):
        yield h % bits
        h = (h + delta) & _MASK32


class BloomFilterBuilder:
    """
    SSTable Bloom Filter Writer
    """

    #: Target bits per inserted key
    bits_per_key: int

    #: Hash probes per key (k)
    num_probes: int

    #: 32-bit hashes collected for the current SSTable
    hashes: list[int]

    def __init__(self, bits_per_key: int = _DEFAULT_BITS_PER_KEY):
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
        """Return the encoded filter size, including the probe-count byte"""
        total_keys = len(self.hashes)
        # m = n * bits_per_key, with a minimum size for small tables
        bits = max(_MIN_FILTER_BITS, total_keys * self.bits_per_key)
        # a whole number of bytes, plus one byte for the probe count
        return (bits + 7) // 8 + 1

    def write(self) -> bytes:
        """
        Pack the current set of keys into an immutable filter

        Encoding:
            [bit array][num_probes]
        """
        nbytes = self.estimated_size() - 1
        bits = nbytes * 8
        array = bytearray(nbytes)

        # Set the same k positions that the reader will test for each key.
        for h in self.hashes:
            for bitpos in _probe(h, bits, self.num_probes):
                array[bitpos // 8] |= 1 << (bitpos % 8)

        array.append(self.num_probes)
        return bytes(array)

    def persist(self) -> bytes:
        """
        Return the finalized filter and reset for the next SSTable

        After compaction (or any new file) the key set changes, so bit array must be rebuilt from scratch
        """
        filter_data = self.write()
        self.hashes = []
        return filter_data


class BloomFilterReader:
    """
    SSTable Bloom Filter Reader

    The filter is immutable
    Missing or too-short filter data is treated as a possible match
    """

    #: Packed filter blob
    data: bytes

    #: Bit-array length in bits
    bits: int

    #: Hash probes per key (k)
    num_probes: int

    def __init__(self, filter_data: bytes):
        self.data = filter_data
        if len(filter_data) < _MIN_FILTER_BITS // 8 + 1:
            self.bits = 0
            self.num_probes = 0
            return

        self.num_probes = filter_data[-1]
        self.bits = (len(filter_data) - 1) * 8

    def may_find(self, key: bytes) -> bool:
        """
        Return True if ``key`` may be present in the SSTable
        False means the key is missing
        """

        # insurance if filter is missing or too short, treat as a possible match
        if self.bits == 0:
            return True

        if not 1 <= self.num_probes <= _MAX_PROBES:
            return True

        array = self.data
        h = crc32(key)
        # One zero bit proves absence
        for bitpos in _probe(h, self.bits, self.num_probes):
            if (array[bitpos // 8] & (1 << (bitpos % 8))) == 0:
                return False
        return True
