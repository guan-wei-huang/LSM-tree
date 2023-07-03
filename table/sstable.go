package table

import "io"

type BlockMetaData struct {
	offset int
	minKey []byte
}

type SSTable struct {
	block    *Block
	firstKey []byte
	offset   int

	blockSize int

	writer io.Writer

	metaData []*BlockMetaData
}

func NewSSTable(writer io.Writer, blockSize int) *SSTable {
	return &SSTable{
		block:     NewBlock(),
		firstKey:  nil,
		offset:    0,
		blockSize: blockSize,
		writer:    writer,
		metaData:  make([]*BlockMetaData, 0),
	}
}

func (s *SSTable) Append(key, val []byte) {
	if s.firstKey == nil {
		s.firstKey = key
	}
	s.block.append(key, val)

	if s.block.estimateSize() >= s.blockSize {
		s.finishBlock()
	}
}

func (s *SSTable) finishBlock() error {
	if err := s.block.finish(); err != nil {
		return err
	}

	n, err := s.writer.Write(s.block.data.Bytes())
	if err != nil {
		return err
	}

	s.metaData = append(s.metaData, &BlockMetaData{
		offset: s.offset,
		minKey: s.firstKey,
	})

	s.offset += n
	s.block.reset()
	s.firstKey = nil

	return nil
}
