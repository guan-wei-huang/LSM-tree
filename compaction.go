package lsm

import (
	"lsm/iterator"
)

type compaction struct {
	level int

	tables [2]tables
}

type compTableBuilder struct {
	s *Storage
	w *tWriter

	tableInfo []*table
}

func (c *compTableBuilder) appendKV(key, val []byte) error {
	if c.w == nil {
		c.w = c.s.newTable()
	}
	c.w.append(key, val)

	return nil
}

func (c *compTableBuilder) needFlush() bool {
	return c.w.estimateSize() >= FileSize
}

func (c *compTableBuilder) flush() error {
	table, err := c.w.finish()
	if err != nil {
		return err
	}
	c.tableInfo = append(c.tableInfo, table)
	c.w = nil

	return nil
}

func (d *DB) majorCompaction(compact *compaction) {
	iters := make([]iterator.Iterator, 0)

	for i, levelFiles := range compact.tables {
		if len(levelFiles) == 0 {
			continue
		}
		if compact.level == 0 && i == 0 {
			for _, file := range levelFiles {
				iters = append(iters, file.newIterator())
			}
		} else {
			idxIter := levelFiles.newIndexIterator(d.cmp)
			iter := iterator.NewTwoLevelIterator(idxIter)
			iters = append(iters, iter)
		}
	}

	compBuilder := &compTableBuilder{
		s:         d.storage,
		w:         nil,
		tableInfo: make([]*table, 0),
	}

	iter := iterator.NewMergeIterator(iters, d.cmp)
	for ; iter.Valid(); iter.Next() {
		compBuilder.appendKV(iter.Key(), iter.Value())
		if compBuilder.needFlush() {
			compBuilder.flush()
		}
	}

	for _, table := range compBuilder.tableInfo {
		d.storage.addTable(compact.level, table)
	}
}
