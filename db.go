package lsm

type DB struct {
	memTable       *MemTable
	immutableTable *MemTable
}

func (d *DB) Put(key, value []byte) {

}

func (d *DB) Get(key []byte) []byte {

}
