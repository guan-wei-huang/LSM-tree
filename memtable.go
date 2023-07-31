package lsm

import (
	"lsm/compare"
	"lsm/iterator"
	"sync"
)

type MemTable struct {
	size  int
	table *SkipList

	mu sync.RWMutex

	// compact only when ref <= 1
	// ref int32
	wg sync.WaitGroup
}

func NewMemTable(cmp compare.Comparator) *MemTable {
	return &MemTable{
		table: NewSkiplist(cmp),
	}
}

func (m *MemTable) Put(key, val []byte) {
	m.mu.Lock()
	m.table.Insert(key, val)
	m.size = m.size + len(key) + len(val)
	m.mu.Unlock()
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	val, ok := m.table.Get(key)
	m.mu.RUnlock()

	return val, ok
}

func (m *MemTable) Scan(lower, upper []byte) *MemTableIterator {
	// TODO
	return nil
}

func (m *MemTable) NewIterator() *MemTableIterator {
	iter := NewSkiplistIterator(m.table)
	return &MemTableIterator{iter}
}

func (m *MemTable) estimateSize() int {
	return m.size
}

func (m *MemTable) ref() {
	m.wg.Add(1)
}

func (m *MemTable) unref() {
	m.wg.Done()
}

func (m *MemTable) wait() {
	m.wg.Wait()
}

type MemTableIterator struct {
	*SkipListIter
}

var _ iterator.Iterator = (*MemTableIterator)(nil)

func NewMemtableIterator(list *SkipList) *MemTableIterator {
	return &MemTableIterator{
		SkipListIter: NewSkiplistIterator(list),
	}
}
