Default block size = 4 KB
Default memTable size = 4 MB

After memTable is full, execute compaction to generate a SSTable,

So, all of key-value in SSTable are sorted, and we can use index block in SSTable to find which block should be loaded into memory

---
assume 2 PUT request concurrent get MemTable, and table is almost full.

need to avoid double compaction

1. add frozen (bool) in memtable
2. add frozen id in DB

---
ref in memtable: add 1 before put

avoid put when executing compaction