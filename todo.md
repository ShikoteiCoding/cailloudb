We keep forgetting what we want to do and we too lazy to maintain a tool for it :)

- Make flush / compaction async
- Add global settings and avoid constants altogether
- Versioning the encoding protocols (wal, sst) for backward compatibility
- Improve fileobj reference to avoid opening file for each write
- Implement durable restart by replaying wal into the store
- Use cheaper encoding for key / value length
- Extend headers metadata (log number etc)

Any XXX / TODO in the code forgotten in the above list is eventually to be addressed