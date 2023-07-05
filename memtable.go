package lsm

import "sync"

type MemTable struct {
	ID uint64

	Table *SkipList

	mu *sync.RWMutex
}

type MemTableIterator struct {
}

func NewMemTable(id uint64) *MemTable {
	return &MemTable{
		ID:    id,
		Table: NewSkiplist(),
	}
}

func (m *MemTable) Put(key, val []byte) {
	m.mu.Lock()
	m.Table.Insert(key, val)
	m.mu.Unlock()
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	val, ok := m.Table.Get(key)
	m.mu.RUnlock()

	return val, ok
}

func (m *MemTable) Scan(lower, upper []byte) *MemTableIterator {

}
