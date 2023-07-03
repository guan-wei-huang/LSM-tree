package lsm

type MemTable struct {
	ID uint64

	Table *SkipList
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
	m.Table.Insert(key, val)
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {

	return nil, false
}

func (m *MemTable) Scan(lower, upper []byte) *MemTableIterator {

}
