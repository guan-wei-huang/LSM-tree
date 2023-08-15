package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"lsm/compare"
	"lsm/iterator"
	cache "lsm/lru-cache"
)

func ErrorNotFound(key []byte) error {
	return fmt.Errorf("key: %v not found", key)
}

/*
table format:

	| block1 | block2 | .. | filter block | index block | filter block offset | filter block len | index block offset | index block len |
*/
type TableWriter struct {
	block       *BlockBuilder
	indexBlock  *BlockBuilder
	filterBlock *FilterBuilder

	firstKey []byte
	offset   int

	// default: 4 KB
	blockSize int

	writer io.WriteCloser
}

func NewTableWriter(writer io.WriteCloser, blockSize int) *TableWriter {
	return &TableWriter{
		block:       NewBlockBuilder(),
		indexBlock:  NewBlockBuilder(),
		filterBlock: NewFilterBuilder(),
		firstKey:    nil,
		offset:      0,
		blockSize:   blockSize,
		writer:      writer,
	}
}

func (s *TableWriter) Append(key, val []byte) {
	if s.firstKey == nil {
		s.firstKey = key
	}

	s.block.append(key, val)
	s.filterBlock.addKey(key)

	if s.block.estimateSize() >= s.blockSize {
		s.finishBlock()
	}
}

func (s *TableWriter) finishBlock() error {
	dataBlock := s.block.build()
	encBlock := encodeBlock(dataBlock)
	n, err := s.writer.Write(encBlock)
	if err != nil {
		return err
	}

	s.filterBlock.arrange()

	s.indexBlock.appendIndex(s.firstKey, s.offset, n)

	s.offset += n
	s.reset()

	return nil
}

func (s *TableWriter) reset() {
	s.block.reset()
	s.firstKey = nil
}

// Write sstable to file
func (s *TableWriter) Flush() (tableSize uint64, err error) {
	if s.firstKey != nil {
		if err := s.finishBlock(); err != nil {
			return 0, err
		}
	}

	filterBlock := s.filterBlock.build()
	encFilterBlock := encodeBlock(filterBlock)
	n1, err := s.writer.Write(encFilterBlock)
	if err != nil {
		return 0, err
	}

	indexBlock := s.indexBlock.build()
	encIndexBlock := encodeBlock(indexBlock)
	n2, err := s.writer.Write(encIndexBlock)
	if err != nil {
		return 0, err
	}

	footer := make([]byte, 16)
	// offset of filter block
	binary.BigEndian.PutUint32(footer[0:4], uint32(s.offset))
	// len of filter block
	binary.BigEndian.PutUint32(footer[4:8], uint32(n1))
	// offset of index block
	binary.BigEndian.PutUint32(footer[8:12], uint32(s.offset+n1))
	// len of index block
	binary.BigEndian.PutUint32(footer[12:16], uint32(n2))

	if _, err = s.writer.Write(footer); err != nil {
		return 0, err
	}

	return uint64(s.offset + n1 + n2 + 16), nil
}

func (s *TableWriter) EstimateSize() int {
	return s.offset + s.block.estimateSize() + s.indexBlock.estimateSize()
}

func (s *TableWriter) Close() {
	s.writer.Close()
}

type TableReader struct {
	r    io.ReaderAt
	size uint64

	cmp compare.Comparator

	indexBlock  *IndexBlock
	filterBlock *FilterBlock

	blockCache cache.Cache
}

func NewTableReader(r io.ReaderAt, cmp compare.Comparator, tableSize uint64, blockCache cache.Cache) (*TableReader, error) {
	reader := &TableReader{
		r:          r,
		cmp:        cmp,
		size:       tableSize,
		blockCache: blockCache,
	}

	footer := make([]byte, 16)
	if _, err := r.ReadAt(footer, int64(tableSize-16)); err != nil {
		return nil, err
	}

	filterOffset := binary.BigEndian.Uint32(footer[:4])
	filterSize := binary.BigEndian.Uint32(footer[4:8])
	filterBlock, err := reader.readBlock(uint64(filterOffset), uint64(filterSize))
	if err != nil {
		return nil, err
	}
	reader.filterBlock = &FilterBlock{
		block: filterBlock,
		bf:    bloomFilter{},
	}

	idxOffset := binary.BigEndian.Uint32(footer[8:12])
	idxSize := binary.BigEndian.Uint32(footer[12:])
	idxBlock, err := reader.readBlock(uint64(idxOffset), uint64(idxSize))
	if err != nil {
		return nil, err
	}
	reader.indexBlock = &IndexBlock{idxBlock}

	return reader, nil
}

func (r *TableReader) Get(key []byte) ([]byte, error) {
	idx := r.indexBlock.seek(r.cmp, key)
	if exist := r.filterBlock.contain(idx, key); !exist {
		return nil, ErrorNotFound(key)
	}

	desc, _ := r.indexBlock.entry(idx)
	if desc == nil {
		return nil, ErrorNotFound(key)
	}

	off, size := decodeIndexEntry(desc)
	block, err := r.readBlock(off, size)
	if err != nil {
		return nil, err
	}

	val, ok := block.get(r.cmp, key)
	if !ok {
		return nil, ErrorNotFound(key)
	}
	return val, nil
}

func (r *TableReader) NewIterator() iterator.Iterator {
	indexIter := NewIndexBlockIterator(r, r.indexBlock)

	return iterator.NewTwoLevelIterator(indexIter)
}

func (r *TableReader) readBlock(offset, size uint64) (*Block, error) {
	block := r.blockCache.Get(offset, func() (interface{}, int64) {
		data := make([]byte, size)
		if _, err := r.r.ReadAt(data, int64(offset)); err != nil {
			// TODO: handle err
			return nil, 0
		}
		return decodeBlock(data), int64(len(data))
	})

	if block == nil {
		return nil, fmt.Errorf("read block error")
	}
	return block.(*Block), nil
}
