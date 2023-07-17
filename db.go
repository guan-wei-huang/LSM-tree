package lsm

import (
	"lsm/sstable"
	"sync"
)

type DB struct {
	nextTableID uint64

	mtable   *MemTable
	immtable *MemTable

	mu sync.RWMutex

	execCompact chan bool
	errCompact  chan error

	storage *Storage

	*journal
}

func New() *DB {
	db := &DB{
		nextTableID: 1,
		mtable:      NewMemTable(0),
		immtable:    nil,
		mu:          sync.RWMutex{},
	}

	go db.goCompaction()

	return db
}

func (d *DB) Put(key, val []byte) {
	d.journal.Write(encodeWriteData(WriteOperationPut, key, val))

	mtable, _ := d.getMemTables(false)
	mtable.Put(key, val)
	mtable.unref()

	if mtable.estimateSize() >= DefaultMemtableSize {
		d.mu.Lock()
		if d.immtable != mtable {
			d.immtable = mtable
			d.mtable = NewMemTable(d.nextTableID)
			d.nextTableID += 1

			d.execCompact <- true
		}
		d.mu.Unlock()
	}

}

func (d *DB) Get(key []byte) []byte {
	mtable, immtable := d.getMemTables(true)
	if val, ok := mtable.Get(key); ok {
		return val
	}

	if immtable != nil {
		if val, ok := immtable.Get(key); ok {
			return val
		}
	}

	if val, ok := d.storage.get(key); ok {
		return val
	}
	return nil
}

func (d *DB) goCompaction() {
	for {
		select {
		case <-d.execCompact:
			if d.immtable != nil {
				d.memCompaction()
				continue
			}
		}
	}
}

func (d *DB) memCompaction() {
	table := d.immtable

	fname := fileName(SstableFile, table.id)
	f, err := openFile(fname, false)
	if err != nil {
		d.errCompact <- err
		return
	}
	defer f.Close()

	// wait other put request done
	table.wait()

	iter := table.NewIterator()
	tableWriter := sstable.NewTableWriter(table.id, f, DefaultBlockSize)
	for ; iter.Valid(); iter.Next() {
		tableWriter.Append(iter.Key(), iter.Value())
	}

	tableSize, err := tableWriter.Flush()
	if err != nil {
		d.errCompact <- err
		return
	}

	d.storage.addTable(table.id, tableSize)

	d.mu.Lock()
	d.immtable = nil
	d.mu.Unlock()
}

func (d *DB) getMemTables(readonly bool) (m, imm *MemTable) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	m = d.mtable
	if !readonly {
		m.ref()
	}

	if d.immtable != nil {
		imm = d.immtable
	}

	return m, imm
}
