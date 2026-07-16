from typing import AsyncIterator

from index import KeyIndex


class DbSnapshot:
    """Read-only point-in-time view pinned to a sequence number."""

    #: Pinned sequence number at snapshot time
    _seq: int

    #: Materialized key → value at pinned seq
    __d: dict[bytes, bytes]

    #: Sorted key index for range scans
    __index: KeyIndex

    def __init__(self, state: dict[bytes, bytes], index: KeyIndex, seq: int):
        self._seq = seq
        self.__d = state
        self.__index = index

    def seq(self) -> int:
        return self._seq

    async def get(self, key: bytes) -> bytes:
        if key not in self.__d:
            raise KeyError("key {} not found".format(key))

        return self.__d[key]

    async def exists(self, key: bytes) -> bool:
        return key in self.__d

    async def scan(
        self,
        start: bytes | None = None,
        end: bytes | None = None,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        for key in self.__index.range(start, end):
            yield key, self.__d[key]
