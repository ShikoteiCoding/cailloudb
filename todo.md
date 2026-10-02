We keep forgetting what we want to do and we too lazy to maintain a tool for it :)


- Integrate the new lsm in the store, sustain feature compatibility (snapshot, wal, txn...)
- Improve fileobj reference to avoid opening file for each write
- Implement durable restart by replaying wal into the store
- Use cheaper encoding for key / value length
- Extend headerds metadata (log number etc)
