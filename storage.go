package lsm

import (
	"lsm/sstable"
	"sync"
)

type Table struct {
	id   uint64
	size uint64
}

func (t *Table) getTableName() string {
	return fileName(SstableFile, t.id)
}

type Storage struct {
	level0 []*Table

	mu sync.RWMutex
}

func NewStorage() *Storage {
	return &Storage{
		level0: make([]*Table, 0),
		mu:     sync.RWMutex{},
	}
}

func (s *Storage) get(key []byte) ([]byte, bool) {
	// TODO: read table when major compacting
	for i := len(s.level0) - 1; i > -1; i-- {
		table := s.level0[i]
		tName := table.getTableName()
		f, err := openFile(tName, true)
		if err != nil {
			// TODO: panic
			return nil, false
		}
		reader, err := sstable.NewTableReader(f, int(table.size))
		if err != nil {
			return nil, false
		}

		if val, err := reader.Get(key); err == nil {
			return val, true
		}
	}

	return nil, false
}

func (s *Storage) addTable(id, size uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.level0 = append(s.level0, &Table{id, size})
}
