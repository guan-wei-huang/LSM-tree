package sstable

import (
	"encoding/binary"
	"io"
)

type BlockMetaData struct {
	offset int
	minKey []byte
}

func EncodeBlockMetaData(b []*BlockMetaData) []byte {
	// like block encoder
	// TODO: after finish close sstable file
	return nil
}

func DecodeMlockMetaData(data []byte) []*BlockMetaData {
	// TODO: after finish close sstable file
	return nil
}

type TableWriter struct {
	block    *BlockBuilder
	firstKey []byte
	offset   int

	// default: 4 KB
	blockSize int

	tableID uint64
	writer  io.Writer

	metaData []*BlockMetaData
}

func NewTableWriter(id uint64, writer io.Writer, blockSize int) *TableWriter {
	return &TableWriter{
		block:     NewBlockBuilder(),
		firstKey:  nil,
		offset:    0,
		blockSize: blockSize,
		tableID:   id,
		writer:    writer,
		metaData:  make([]*BlockMetaData, 0),
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
	encBlock := EncodeBlock(s.block.build())
	n, err := s.writer.Write(encBlock)
	if err != nil {
		return err
	}

	s.metaData = append(s.metaData, &BlockMetaData{
		offset: s.offset,
		minKey: s.firstKey,
	})

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

	encMetaData := EncodeBlockMetaData(s.metaData)
	n, err := s.writer.Write(encMetaData)
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
	r *io.ReaderAt
}

func NewTableReader() *TableReader {

}
