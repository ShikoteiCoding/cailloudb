We keep forgetting what we want to do and we too lazy to maintain a tool for it :)


- Implement LSM tree and bundle memtable + SST
- Integrate the new lsm in the store, sustain feature compatibility (snapshot, wal, txn...)
- Add global settings and avoid constants altogether
- Versioning the encoding protocols (wal, sst) for backward compatibility