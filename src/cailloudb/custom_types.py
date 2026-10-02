TOMBSTONE = b"__INTERNAL_TOMBSTONE_MARKER__"


class SeqNum:
    """Monotonically increasing sequencer generator."""

    #: Last sequence number
    _value: int

    def __init__(self):
        self._value = 0

    def increment(self):
        self._value += 1

    def __int__(self) -> int:
        return self._value
