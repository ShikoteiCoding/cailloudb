We keep forgetting what we want to do and we too lazy to maintain a tool for it :)

- Refactor the root dir for storage, adopt a hierarchical file system
- Implement durable restart by replaying wal into the store
- Use cheaper encoding for key / value length
- Extend headers metadata (log number etc)
- Introduce crash recovery
- Implement last active snapshot guardrail during compaction
- Drop tombstones (hard delete) on last level during compaction
- Implement bloom filter
- Solve memtable flush queuing concurrency issues
- Optimize CPU-costly read/merge paths
- Add retry strategy and raise on max retry
- Replace naive compaction target-size heuristic
- Decide whether single-op WAL records should keep no batch header
- Versioning the encoding protocols (wal, sst) for backward compatibility

Any XXX / TODO in the code forgotten in the above list is eventually to be addressed
