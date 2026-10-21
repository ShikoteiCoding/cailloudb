We keep forgetting what we want to do and we too lazy to maintain a tool for it :)

- Make flush / compaction running in the background
- Add global settings and avoid constants altogether
- Improve fileobj reference to avoid opening file for each write
- Implement durable restart by replaying wal into the store
- Use cheaper encoding for key / value length
- Extend headers metadata (log number etc)
- Implement last active snapshot guardrail during compaction
- Drop tombstones (hard delete) on last level during compaction
- Implement bloom filter
- Solve memtable flush queuing concurrency issues
- Optimize CPU-costly read/merge paths
- Add retry strategy and raise on max retry
- Lock levels / map during compaction operations
- Resolve next file id concurrency during flush
- Replace naive compaction target-size heuristic
- Rotate WAL files to wal-(n+1) instead of truncating a single file
- Decide whether single-op WAL records should keep no batch header
- Versioning the encoding protocols (wal, sst) for backward compatibility

Any XXX / TODO in the code forgotten in the above list is eventually to be addressed