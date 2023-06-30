package lsm

import "fmt"

type TableMetaData struct {
	tableID uint64
}

func (t *TableMetaData) GetFileName() string {
	return fmt.Sprintf("sstable-%v", t.tableID)
}

type TableManager struct {
	tables []*TableMetaData
}

func NewTableManager() *TableManager {
	return &TableManager{
		tables: make([]*TableMetaData, 0),
	}
}
