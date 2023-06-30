package lsm

type MemTable struct {
	ID uint64

	Table *SkipList
}

func NewMemTable(id uint64) *MemTable {
	return &MemTable{
		ID:    id,
		Table: NewSkiplist(),
	}
}

func (m *MemTable) Put(key, value []byte) {

}

func (m *MemTable) Get(key []byte) []byte {

}
