We keep forgetting what we want to do and we too lazy to maintain a tool for it :)

- Have MemTable MVCC -> store seq, follow Version-in-Key approach by implementing a composite key (key, version)
- Implement in memory SST and bundle memtable inside the lsm
- Integrate the new lsm in the store
- Improve fileobj reference to avoid opening file for each write
- Implement durable restart by replaying wal into the store
- Use cheaper encoding for key / value length
- Extend headerds metadata (log number etc)
