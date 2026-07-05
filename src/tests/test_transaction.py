import pytest

from cailloudb import ObjectStore, DbBuilder


@pytest.mark.asyncio
async def test_transaction_commit():
    store = ObjectStore.resolve(":memory:")
    db = DbBuilder("test-db", store).build()

    await db.put(b"old", b"1")

    txn = db.begin()
    await txn.put(b"k", b"v")
    await txn.delete(b"old")
    await txn.commit()

    assert await db.get(b"k") == b"v"
    with pytest.raises(KeyError):
        await db.get(b"old")

    assert await db.latest_sequence_number() == 3
