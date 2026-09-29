We keep forgetting what we want to do and we too lazy to maintain a tool for it :)

- Have MemTable MVCC -> store seq and timestamps, follow Version-in-Key approach by implementing a composite key (key, version)
- Implement in memory SST and bundle memtable inside the lsm
- Integrate the new lsm in the store