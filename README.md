Default block size = 4 KB
Default memTable size = 4 MB

After memTable is full, execute compaction to generate a SSTable,

So, all of key-value in SSTable are sorted, and we can use index block in SSTable to find which block should be loaded into memory