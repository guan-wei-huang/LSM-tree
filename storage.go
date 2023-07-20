package lsm

import (
	"lsm/iterator"
	"lsm/sstable"
	"os"
	"sync"
)

type Table struct {
	id   uint64
	size int
}

func (t *Table) getTableName() string {
	return fileName(SstableFile, t.id)
}

func (t *Table) open(readOnly bool) (*os.File, error) {
	name := t.getTableName()
	f, err := openFile(name, readOnly)
	if err != nil {
		return nil, err
	}
	return f, nil
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
		f, err := table.open(true)
		if err != nil {
			// TODO: panic
			return nil, false
		}
		reader, err := sstable.NewTableReader(f, table.size)
		if err != nil {
			return nil, false
		}

		if val, err := reader.Get(key); err == nil {
			return val, true
		}
	}

	return nil, false
}

func (s *Storage) getIterator() []iterator.Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	iters := make([]iterator.Iterator, len(s.level0))
	for i, table := range s.level0 {
		f, err := table.open(true)
		if err != nil {
			// TODO: panic
			continue
		}

		reader, err := sstable.NewTableReader(f, table.size)
		if err != nil {
			continue
		}

		iters[i] = reader.NewIterator()
	}
	return iters
}

func (s *Storage) addTable(id, size uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.level0 = append(s.level0, &Table{id, int(size)})
}
