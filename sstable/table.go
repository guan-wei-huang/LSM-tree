package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"lsm/compare"
)

func ErrorNotFound(key []byte) error {
	return fmt.Errorf("key: %v not found", key)
}

/*
table format:

	| block1 | block2 | .. | index block |  index block offset | index block len |
*/
type TableWriter struct {
	block    *BlockBuilder
	firstKey []byte
	offset   int

	// default: 4 KB
	blockSize int

	tableID uint64
	writer  io.Writer

	indexBlock *BlockBuilder
}

func NewTableWriter(id uint64, writer io.Writer, blockSize int) *TableWriter {
	return &TableWriter{
		block:      NewBlockBuilder(),
		firstKey:   nil,
		offset:     0,
		blockSize:  blockSize,
		tableID:    id,
		writer:     writer,
		indexBlock: NewBlockBuilder(),
	}
}

func (s *TableWriter) Append(key, val []byte) {
	if s.firstKey == nil {
		s.firstKey = make([]byte, len(key))
		copy(s.firstKey, key)
	}
	s.block.append(key, val)

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

	s.indexBlock.appendIndex(s.firstKey, s.offset, n)

	s.offset += n
	s.Reset()

	return nil
}

func (s *TableWriter) Reset() {
	s.block.reset()
	s.firstKey = nil
}

// Write sstable to file
func (s *TableWriter) Flush() (uint64, error) {
	if s.firstKey != nil {
		if err := s.finishBlock(); err != nil {
			return 0, err
		}
	}

	indexBlock := s.indexBlock.build()
	encIndexBlock := encodeBlock(indexBlock)
	n, err := s.writer.Write(encIndexBlock)
	if err != nil {
		return 0, err
	}

	footer := make([]byte, 8)
	// start index of meta block
	binary.BigEndian.PutUint32(footer[0:4], uint32(s.offset))
	// len of meta block
	binary.BigEndian.PutUint32(footer[4:8], uint32(n))
	if _, err = s.writer.Write(footer); err != nil {
		return 0, err
	}

	return uint64(s.offset + n + 8), nil
}

type TableReader struct {
	r    io.ReaderAt
	size int

	cmp compare.Comparator

	indexBlock *IndexBlock
}

func NewTableReader(r io.ReaderAt, tableSize int) (*TableReader, error) {
	footer := make([]byte, 8)
	if _, err := r.ReadAt(footer, int64(tableSize-8)); err != nil {
		return nil, err
	}
	idxOffset := binary.BigEndian.Uint32(footer[:4])
	idxSize := binary.BigEndian.Uint32(footer[4:8])

	idxBlock, err := readBlock(r, idxOffset, idxSize)
	if err != nil {
		return nil, err
	}

	reader := &TableReader{
		r:          r,
		size:       tableSize,
		indexBlock: &IndexBlock{idxBlock},
	}
	return reader, nil
}

func (r *TableReader) Get(key []byte) ([]byte, error) {
	idx := r.indexBlock.seek(r.cmp, key)
	meta, _ := r.indexBlock.entry(idx)
	if meta == nil {
		return nil, ErrorNotFound(key)
	}

	off, size := decodeIndexBlock(meta)

	block, err := readBlock(r.r, uint32(off), uint32(size))
	if err != nil {
		return nil, err
	}

	// TODO: due to bloom filter, its only necessary to check for 'ok'
	val, ok := block.get(r.cmp, key)
	if !ok {
		return nil, ErrorNotFound(key)
	}
	return val, nil
}
