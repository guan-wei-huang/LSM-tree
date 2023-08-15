package lsm

import (
	"lsm/iterator"
)

type compactRange struct {
	level  int
	minKey []byte
	maxKey []byte
}

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

// finish clean buffer and return table
func (c *compTableBuilder) finish() ([]*table, error) {
	if c.w.estimateSize() > 0 {
		if err := c.flush(); err != nil {
			return nil, err
		}
	}
	return c.tableInfo, nil
}

func (d *DB) goCompaction() {
	for {
		select {
		case <-d.pauseChan:
			// wait until resume
			<-d.pauseChan
		case <-d.memCompact:
			if d.immtable != nil {
				d.memCompaction()
				continue
			}
		case cRange := <-d.levelCompact:
			// TODO: record compaction history, to avoid trigger redundant compaction
			if !d.storage.checkLevelCompaction(cRange.level) {
				continue
			}
			compact := d.storage.pickCompaction(cRange.level)
			d.majorCompaction(compact)
		}
	}
}

func (d *DB) majorCompaction(compact *compaction) {
	iters := make([]iterator.Iterator, 0)

	for i, levelFiles := range compact.tables {
		if len(levelFiles) == 0 {
			continue
		}
		if compact.level == 0 && i == 0 {
			for _, table := range levelFiles {
				iters = append(iters, d.storage.newIterator(table))
			}
		} else {
			idxIter := levelFiles.newIndexIterator(d.storage, d.cmp)
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
			if err := compBuilder.flush(); err != nil {
				panic(err)
			}
		}
	}

	newTables, err := compBuilder.finish()
	if err != nil {
		// TODO
		panic(err)
	}

	delTables := make([]*table, 0)
	for _, tables := range compact.tables {
		for _, t := range tables {
			delTables = append(delTables, t)
		}
	}
	d.storage.applyCompaction(compact.level, newTables, delTables)
}

func (d *DB) memCompaction() {
	table := d.immtable

	// wait other put request done
	// TODO: combine multiple put requests into 1 thread, then it can avoid trigger many times of compaction
	table.wait()

	iter := table.NewIterator()
	tWriter := d.storage.newTable()
	for ; iter.Valid(); iter.Next() {
		tWriter.append(iter.Key(), iter.Value())
	}

	tInfo, err := tWriter.finish()
	if err != nil {
		d.errCompact <- err
		return
	}

	d.storage.addTable(0, tInfo)

	d.mu.Lock()
	d.immtable = nil
	d.mu.Unlock()
}
