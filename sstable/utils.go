package sstable

import (
	"encoding/binary"
	"io"

	"golang.org/x/exp/constraints"
)

func max[T constraints.Ordered](a, b T) T {
	if a >= b {
		return a
	}
	return b
}

func min[T constraints.Ordered](a, b T) T {
	if a <= b {
		return a
	}
	return b
}

func readBlock(r io.ReaderAt, offset, size uint32) (*Block, error) {
	data := make([]byte, size)
	if _, err := r.ReadAt(data, int64(offset)); err != nil {
		return nil, err
	}

	return decodeBlock(data), nil
}

func encodeBlock(b *Block) []byte {
	// TODO: reuse slice
	sizeBuf := make([]byte, 4*len(b.offset)+4)
	for i, off := range b.offset {
		binary.BigEndian.PutUint32(sizeBuf[4*i:], off)
	}
	binary.BigEndian.PutUint32(sizeBuf[4*len(b.offset):], uint32(len(b.offset)))

	b.data = append(b.data, sizeBuf...)
	return b.data
}

func decodeBlock(data []byte) *Block {
	size := len(data)
	num := int(binary.BigEndian.Uint32(data[size-4:]))
	offset := make([]uint32, num)
	offsetIdx := size - 4 - 4*num
	for i := 0; i < num; i += 1 {
		offset[i] = binary.BigEndian.Uint32(data[offsetIdx+4*i:])
	}

	return &Block{
		data:   data[:offsetIdx],
		offset: offset,
	}
}

func decodeIndexBlock(data []byte) (offset uint64, len uint64) {
	offset, n1 := binary.Uvarint(data[0:])
	len, _ = binary.Uvarint(data[n1:])
	return
}
