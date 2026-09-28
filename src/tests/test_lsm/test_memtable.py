from cailloudb.lsm.memtable import MemTable


def test_skip_list_put_and_get():
    mem_table = MemTable()

    mem_table.put(b"test1", b"val1")

    assert len(mem_table) == 1
    assert mem_table.get(b"test1") == b"val1"
    assert mem_table.get(b"test2") is None


def test_skip_list_put_and_delete():
    mem_table = MemTable()

    mem_table.put(b"test1", b"val1")

    assert len(mem_table) == 1
    assert mem_table.get(b"test1") == b"val1"
    assert mem_table.get(b"test2") is None

    mem_table.delete(b"test1")
    assert len(mem_table) == 0
    assert mem_table.get(b"test1") is None
