package lsm

import (
	"sync"
)

type Table struct {
	id   uint64
	size uint64
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

	}
}

func (s *Storage) addTable(id, size uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.level0 = append(s.level0, &Table{id, size})
}
