from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lsm.version import Version


class EngineSnapshot:
    #: Sequence number at creation time
    _seq_num: int

    #: Pinned version
    _version: Version

    def __init__(self, seq_num: int, version: Version):
        self._seq_num = seq_num
        self._version = version

    def get_pinned_sequence_number(self) -> int:
        return self._seq_num

    def get_pinned_version(self) -> Version:
        return self._version
