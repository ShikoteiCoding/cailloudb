We keep forgetting what we want to do and we too lazy to maintain a tool for it :)


- Integrate the new lsm in the store, sustain feature compatibility (snapshot, wal, txn...)
- Implement flush compaction + expose a public store API for it
- Introduce page blocks for sstables
- Make use of size in skiplist and sstables to handle full blocks and spilled blocks
- Add global settings and avoid constants altogether
- Versioning the encoding protocols (wal, sst) for backward compatibility
- Improve fileobj reference to avoid opening file for each write
- Implement durable restart by replaying wal into the store
- Use cheaper encoding for key / value length
- Extend headers metadata (log number etc)
