package lsm

import (
	"lsm/compare"
	"lsm/iterator"
	"sync"
)

type DB struct {
	mtable   *MemTable
	immtable *MemTable

	storage *Storage
	journal *journal
	cmp     compare.Comparator

	mu sync.RWMutex

	memCompact   chan bool
	levelCompact chan compactRange
	errCompact   chan error

	// for testing
	pauseChan chan struct{}
}

func New() *DB {
	db := &DB{
		memCompact:   make(chan bool, 3),
		levelCompact: make(chan compactRange, 5),
		errCompact:   make(chan error),
		pauseChan:    make(chan struct{}),

		cmp: DefaultComparator,
	}
	db.storage = NewStorage(db)
	db.newMem()

	go db.goCompaction()

	return db
}

func (d *DB) Put(key, val []byte) {
	d.journal.WriteRecord(WriteOperationPut, key, val)

	mtable, _ := d.getMemTables(false)
	mtable.Put(key, val)
	mtable.unref()

	if mtable.estimateSize() >= DefaultMemtableSize {
		d.mu.Lock()
		if d.immtable != mtable {
			d.frozenMem()
			d.newMem()

			d.memCompact <- true
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

func (d *DB) NewIterator() iterator.Iterator {
	iters := make([]iterator.Iterator, 0)

	mtable, immtable := d.getMemTables(true)
	iters = append(iters, mtable.NewIterator())

	if immtable != nil {
		iters = append(iters, immtable.NewIterator())
	}

	iters = append(iters, d.storage.getIterators()...)

	mergeIter := iterator.NewMergeIterator(iters, d.cmp)
	return mergeIter
}

func (d *DB) frozenMem() {
	d.immtable = d.mtable
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

func (d *DB) newMem() {
	d.mtable = NewMemTable(d.cmp)

	id := d.storage.newFileId()
	f, err := openFile(fileName(LogFile, id), false)
	if err != nil {
		panic(err)
	}

	if d.journal == nil {
		d.journal = NewJournal(f)
	} else {
		d.journal.Reset(f)
	}
}
