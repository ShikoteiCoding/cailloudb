from lsm.sstable import SSTable, SSTableWriter


class SSTableCompactor:
    """
    Compact multiple SSTables into a single SSTable.
    """

    def __init__(self): ...

    def compact(
        self, sstables: list[SSTable], sstable_writer: SSTableWriter
    ) -> SSTable: ...
