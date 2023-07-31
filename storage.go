package lsm

import (
	"lsm/compare"
	"lsm/iterator"
	"lsm/sstable"
	"os"
	"sync"
)

type tWriter struct {
	id uint64
	w  *sstable.TableWriter

	minKey, maxKey []byte
}

func (t *tWriter) estimateSize() int {
	return t.w.EstimateSize()
}

func (t *tWriter) append(key, val []byte) {
	if t.minKey == nil {
		t.minKey = append([]byte(nil), key...)
	}
	t.maxKey = append([]byte(nil), key...)

	t.w.Append(key, val)
}

func (t *tWriter) finish() (*table, error) {
	size, err := t.w.Flush()
	if err != nil {
		return nil, err
	}
	t.w.Close()

	tt := &table{
		id:     t.id,
		size:   int(size),
		minKey: t.minKey,
		maxKey: t.maxKey,
	}
	return tt, nil
}

type table struct {
	id   uint64
	size int

	minKey, maxKey []byte
}

func (t *table) getTableName() string {
	return fileName(SstableFile, t.id)
}

func (t *table) open(readOnly bool) (*os.File, error) {
	name := t.getTableName()
	f, err := openFile(name, readOnly)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (t *table) newIterator() iterator.Iterator {
	f, err := t.open(true)
	if err != nil {
		// TODO: error handling
		return nil
	}

	reader, err := sstable.NewTableReader(f, t.size)
	if err != nil {
		return nil
	}

	return reader.NewIterator()
}

type tables []*table

func (t *tables) newIndexIterator(cmp compare.Comparator) iterator.IndexIterator {
	return newLevelFilesIterator(*t, cmp)
}

type levelFilesIterator struct {
	cmp compare.Comparator

	tables
	idx int
}

func newLevelFilesIterator(ts tables, cmp compare.Comparator) *levelFilesIterator {
	iter := levelFilesIterator{cmp, ts, 0}
	return &iter
}

func (i *levelFilesIterator) First() {
	i.idx = 0
}

func (i *levelFilesIterator) Next() {
	if !i.Valid() {
		return
	}
	i.idx += 1
}

func (i *levelFilesIterator) Prev() {
	if !i.Valid() {
		return
	}
	i.idx -= 1
}

func (i *levelFilesIterator) Seek(key []byte) {
	for idx, t := range i.tables {
		if i.cmp.Compare(t.minKey, key) <= 0 && i.cmp.Compare(t.maxKey, key) >= 0 {
			i.idx = idx
			return
		} else if i.cmp.Compare(t.minKey, key) > 0 {
			break
		}
	}
	i.idx = -1
}

func (i *levelFilesIterator) Valid() bool {
	return i.idx >= 0 && i.idx < len(i.tables)
}

func (i *levelFilesIterator) Key() []byte {
	return nil
}

func (i *levelFilesIterator) Value() []byte {
	return nil
}

func (i *levelFilesIterator) Get() iterator.Iterator {
	if !i.Valid() {
		return nil
	}
	return i.tables[i.idx].newIterator()
}

type Storage struct {
	db *DB

	level0 []*table
	levels [][]*table

	mu sync.RWMutex

	nextFileId uint64
}

func NewStorage(db *DB) *Storage {
	return &Storage{
		db:     db,
		level0: make([]*table, 0),
		levels: make([][]*table, 0),
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

func (s *Storage) newIterator() []iterator.Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	iters := make([]iterator.Iterator, len(s.level0))
	for i, table := range s.level0 {
		iters[i] = table.newIterator()
	}
	return iters
}

func (s *Storage) newTable() *tWriter {
	tid := s.newFileId()
	tFile, err := openFile(fileName(SstableFile, tid), false)
	if err != nil {
		// TODO: panic
		return nil
	}

	w := sstable.NewTableWriter(tFile, DefaultBlockSize)
	return &tWriter{
		id: tid,
		w:  w,
	}
}

func (s *Storage) addTable(level int, t *table) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if level == 0 {
		s.level0 = append(s.level0, t)
	} else {
		if len(s.levels) < level {
			s.levels = append(s.levels, []*table{})
		}
		s.levels[level-1] = append(s.levels[level-1], t)
	}

	s.checkCompaction()
}

func (s *Storage) checkCompaction() {
	if len(s.level0) > Level0FileNumber {
		s.db.levelCompact <- 0
		return
	}

	for level, tables := range s.levels {
		totalSize := 0
		for _, t := range tables {
			totalSize += t.size
		}
		if totalSize >= levelFilesSize(level+1) {
			s.db.levelCompact <- level
			return
		}
	}
}

func (s *Storage) peekCompaction(level int) *compaction {
	comp := compaction{
		level: level,
	}

	flevel := make(tables, 0)
	if level == 0 {
		flevel = append(flevel, s.level0...)
	} else {
		flevel = append(flevel, s.levels[level][0])
	}

	minKey, maxKey := flevel[0].minKey, flevel[0].maxKey
	for _, t := range comp.tables[0] {
		if s.db.cmp.Compare(t.minKey, minKey) < 0 {
			minKey = t.minKey
		}
		if s.db.cmp.Compare(t.maxKey, maxKey) > 0 {
			maxKey = t.maxKey
		}
	}

	comp.tables[0] = flevel
	comp.tables[1] = append(comp.tables[1], s.overlapTables(level+1, minKey, maxKey)...)

	return &comp
}

func (s *Storage) overlapTables(level int, minKey, maxKey []byte) []*table {
	tables := make([]*table, 0)
	for _, t := range s.levels[level-1] {
		if !(s.db.cmp.Compare(t.minKey, maxKey) > 0 || s.db.cmp.Compare(t.maxKey, minKey) < 0) {
			tables = append(tables, t)
		}
	}
	return tables
}

func (s *Storage) newFileId() uint64 {
	id := s.nextFileId
	s.nextFileId += 1
	return id
}
