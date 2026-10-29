"""
Bloom filter (full filter, one per SSTable)
    [bit array][num_probes: 1 byte]

The filter is immutable once finished. After compaction (or any new
SSTable) the builder is reset and a new array is allocated, because the
set of keys in the file has changed.
"""

DEFAULT_BITS_PER_KEY = 10
# BloomHash seed
_BLOOM_HASH_SEED = 0xBC9F1D34
_HASH_M = 0xC6A4A793
_HASH_R = 24
_MASK32 = 0xFFFFFFFF
_MIN_FILTER_BITS = 64
_MAX_PROBES = 30


def _hash(data: bytes, seed: int) -> int:
    n = len(data)
    h = (seed ^ ((n * _HASH_M) & _MASK32)) & _MASK32

    i = 0
    aligned = n - (n % 4)
    while i < aligned:
        w = int.from_bytes(data[i : i + 4], "little")
        h = (h + w) & _MASK32
        h = (h * _HASH_M) & _MASK32
        h ^= h >> 16
        i += 4

    leftover = n - i
    if leftover:
        if leftover >= 3:
            h = (h + (data[i + 2] << 16)) & _MASK32
        if leftover >= 2:
            h = (h + (data[i + 1] << 8)) & _MASK32
        h = (h + data[i]) & _MASK32
        h = (h * _HASH_M) & _MASK32
        h ^= h >> _HASH_R

    return h


def _num_probes_for(bits_per_key: int) -> int:
    # k = bits_per_key * ln(2) ≈ bits_per_key * 0.69, truncated
    probes = int(bits_per_key * 0.69)
    if probes < 1:
        return 1
    if probes > _MAX_PROBES:
        return _MAX_PROBES
    return probes


def _probe(h: int, bits: int, num_probes: int):
    delta = ((h >> 17) | ((h << 15) & _MASK32)) & _MASK32
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

    def __init__(self, bits_per_key: int = DEFAULT_BITS_PER_KEY):
        self.bits_per_key = bits_per_key
        self.num_probes = _num_probes_for(bits_per_key)
        self.hashes: list[int] = []

    def add_key(self, key: bytes) -> None:
        """
        Record a user key for the current SSTable's filter.
        """
        self.hashes.append(_hash(key, _BLOOM_HASH_SEED))

    def estimated_size(self) -> int:
        """Return the encoded filter size, including the probe-count byte."""
        bits = max(_MIN_FILTER_BITS, len(self.hashes) * self.bits_per_key)
        return (bits + 7) // 8 + 1

    def write(self) -> bytes:
        """
        Pack the current set of keys into an immutable filter blob.

        Encoding:
            [bit array][num_probes]
        """
        nbytes = self.estimated_size() - 1
        bits = nbytes * 8
        array = bytearray(nbytes)

        for h in self.hashes:
            for bitpos in _probe(h, bits, self.num_probes):
                array[bitpos // 8] |= 1 << (bitpos % 8)

        array.append(self.num_probes)
        return bytes(array)

    def finish(self) -> bytes:
        """
        Return the finalized filter and reset for the next SSTable.

        After compaction (or any new file) the key set changes, so the
        bit array must be rebuilt from scratch.
        """
        filter_data = self.write()
        self.hashes = []
        return filter_data


class BloomFilterReader:
    """
    SSTable Bloom Filter Reader.

    The filter is immutable. False positives are possible; a negative
    result means the key is definitely not in this SSTable.
    Missing or too-short filter data is treated as a possible match.
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
        Return True if ``key`` may be present in the SSTable.

        False means the key is definitely absent.
        """
        if self.bits == 0:
            return True

        # Unknown probe counts (including newer encodings) are treated as a match.
        if not 1 <= self.num_probes <= _MAX_PROBES:
            return True

        array = self.data
        h = _hash(key, _BLOOM_HASH_SEED)
        for bitpos in _probe(h, self.bits, self.num_probes):
            if (array[bitpos // 8] & (1 << (bitpos % 8))) == 0:
                return False
        return True
