import bisect
from collections.abc import Iterator


class SortedKeyIndex:
    """Sorted byte-key index"""

    _keys: list[bytes]

    def __init__(self):
        self._keys = []

    def insert(self, key: bytes) -> None:
        idx = bisect.bisect_left(self._keys, key)
        if idx < len(self._keys) and self._keys[idx] == key:
            return
        bisect.insort(self._keys, key)

    def remove(self, key: bytes) -> None:
        idx = bisect.bisect_left(self._keys, key)
        if idx < len(self._keys) and self._keys[idx] == key:
            del self._keys[idx]

    def range(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> Iterator[bytes]:
        lo = bisect.bisect_left(self._keys, start) if start is not None else 0
        hi = bisect.bisect_left(self._keys, end) if end is not None else len(self._keys)
        yield from self._keys[lo:hi]
