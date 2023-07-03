package table

import (
	"bytes"
	"encoding/binary"
)

type Block struct {
	data    bytes.Buffer
	offsets []uint32

	temp []byte
}

func NewBlock() *Block {
	buf := make([]byte, 0)
	return &Block{
		data:    *bytes.NewBuffer(buf),
		offsets: make([]uint32, 0),
		temp:    make([]byte, 30),
	}
}

func (b *Block) append(key, val []byte) error {
	b.offsets = append(b.offsets, uint32(b.data.Len()))

	n := binary.PutUvarint(b.temp, uint64(len(key)))
	n += binary.PutUvarint(b.temp[n:], uint64(len(val)))
	if _, err := b.data.Write(b.temp); err != nil {
		return err
	}
	if _, err := b.data.Write(key); err != nil {
		return err
	}
	if _, err := b.data.Write(val); err != nil {
		return err
	}
	return nil
}

func (b *Block) estimateSize() int {
	return b.data.Len() + 4*len(b.offsets) + 4
}

func (b *Block) finish() error {
	sizeBuf := make([]byte, 4*len(b.offsets)+4)
	n := 0
	for _, offset := range b.offsets {
		n += binary.PutUvarint(sizeBuf[n:], uint64(offset))
	}
	binary.PutUvarint(sizeBuf[n:], uint64(len(b.offsets)))

	if _, err := b.data.Write(sizeBuf); err != nil {
		return err
	}
	return nil
}

func (b *Block) reset() {
	b.data.Reset()
	b.offsets = b.offsets[:0]
}

type BlockIterator struct {
	block  *Block
	curIdx int
}

func NewBlockIterator(b *Block) *BlockIterator {
	b.data.Reset()
	return &BlockIterator{b, 0}
}

func (i *BlockIterator) Next() {
	i.curIdx += 1
}

func (i *BlockIterator) Value() []byte {

}

func (i *BlockIterator) Valid() bool {
	// return i.curIdx < i.block.num
}
