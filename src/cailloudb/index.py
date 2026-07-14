import bisect
from collections.abc import Iterable, Iterator


class KeyIndex:
    #: Sorted byte-key index

    _keys: list[bytes]

    def __init__(self):
        self._keys = []

    @classmethod
    def from_keys(cls, keys: Iterable[bytes]) -> "KeyIndex":
        index = cls()
        index._keys = sorted(keys)
        return index

    def insert(self, key: bytes) -> None:
        idx = bisect.bisect_left(self._keys, key)
        if idx < len(self._keys) and self._keys[idx] == key:
            return
        self._keys.insert(idx, key)

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
