package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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
	encBlock := EncodeBlock(dataBlock)
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
	encIndexBlock := EncodeBlock(indexBlock)
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

	indexBlock *IndexBlock
}

func NewTableReader(r io.ReaderAt, tableSize int) (*TableReader, error) {
	footer := make([]byte, 8)
	if _, err := r.ReadAt(footer, int64(tableSize-8)); err != nil {
		return nil, err
	}
	idxOffset := binary.BigEndian.Uint32(footer[:4])
	idxSize := binary.BigEndian.Uint32(footer[4:8])

	idxBlock, err := readBlock(r, idxOffset, idxSize, tableSize)
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

func readBlock(r io.ReaderAt, offset, size uint32, tableSize int) (*Block, error) {
	data := make([]byte, size)
	if _, err := r.ReadAt(data, int64(offset)); err != nil {
		return nil, err
	}

	return DecodeBlock(data), nil
}

func (r *TableReader) Get(key []byte) ([]byte, error) {
	// smaller than table's min key
	minKey, _, _, _ := r.indexBlock.entry(0)
	if bytes.Compare(key, minKey) < 0 {
		return nil, ErrorNotFound(key)
	}

	idx := r.indexBlock.seek(key)
	_, off, size, _ := r.indexBlock.entry(idx)

	block, err := readBlock(r.r, uint32(off), uint32(size), r.size)
	if err != nil {
		return nil, err
	}

	// TODO: due to bloom filter, its only necessary to check for 'ok'
	val, ok := block.get(key)
	if !ok {
		return nil, ErrorNotFound(key)
	}
	return val, nil
}
