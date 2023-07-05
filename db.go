package lsm

import "sync"

type DB struct {
	memTable       *MemTable
	immutableTable *MemTable

	mutex *sync.RWMutex
}

func (d *DB) Put(key, value []byte) {

}

func (d *DB) Get(key []byte) []byte {

}
