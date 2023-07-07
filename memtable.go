package lsm

import (
	"sync"
)

type MemTable struct {
	id    uint64
	size  int
	table *SkipList

	mu sync.RWMutex

	// compact only when ref <= 1
	// ref int32
	wg sync.WaitGroup
}

type MemTableIterator struct {
}

func NewMemTable(id uint64) *MemTable {
	return &MemTable{
		id:    id,
		table: NewSkiplist(),
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

}

func (m *MemTable) NewIterator() *MemTableIterator {

}

func (m *MemTable) estimateSize() int {
	return m.size
}

func (m *MemTable) ref() {
	m.wg.Add(1)
	// atomic.AddInt32(&m.ref, 1)
}

func (m *MemTable) unref() {
	m.wg.Done()
	// atomic.AddInt32(&m.ref, -1)
}

func (m *MemTable) wait() {
	m.wg.Wait()
}

func (i *MemTableIterator) First() {

}

func (i *MemTableIterator) Next() {

}

func (i *MemTableIterator) Valid() bool {

}

func (i *MemTableIterator) Key() []byte {

}

func (i *MemTableIterator) Value() []byte {

}
