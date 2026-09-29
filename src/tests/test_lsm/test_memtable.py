from cailloudb.custom_types import TOMBSTONE
from cailloudb.lsm.memtable import MemTable


def test_skip_list_put_and_get():
    mem_table = MemTable()

    mem_table.put(b"test1", b"val1")

    assert len(mem_table) == 1
    assert mem_table.bytes_size == len(b"test1") + len(b"val1")
    assert mem_table.get(b"test1") == b"val1"
    assert mem_table.get(b"test2") is None


def test_skip_list_put_and_delete():
    mem_table = MemTable()

    mem_table.put(b"test1", b"val1")

    assert len(mem_table) == 1
    assert mem_table.bytes_size == len(b"test1") + len(b"val1")
    assert mem_table.get(b"test1") == b"val1"
    assert mem_table.get(b"test2") is None

    mem_table.delete(b"test1")
    # Deletes for now just replace value with deletion marker
    assert len(mem_table) == 1
    assert mem_table.bytes_size == len(b"test1") + len(TOMBSTONE)
    assert mem_table.get(b"test1") == TOMBSTONE
