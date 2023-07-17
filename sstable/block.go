package sstable

import (
	"bytes"
	"encoding/binary"
	"lsm/compare"
	"lsm/iterator"
)

type BlockBuilder struct {
	data    bytes.Buffer
	offsets []uint32

	temp []byte
}

func NewBlockBuilder() *BlockBuilder {
	buf := make([]byte, 0)
	return &BlockBuilder{
		data:    *bytes.NewBuffer(buf),
		offsets: make([]uint32, 0),
		temp:    make([]byte, 30),
	}
}

func (b *BlockBuilder) append(key, val []byte) error {
	b.offsets = append(b.offsets, uint32(b.data.Len()))

	n := binary.PutUvarint(b.temp, uint64(len(key)))
	n += binary.PutUvarint(b.temp[n:], uint64(len(val)))
	if _, err := b.data.Write(b.temp[:n]); err != nil {
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

func (b *BlockBuilder) appendIndex(minKey []byte, blockOffset, blockLen int) error {
	b.offsets = append(b.offsets, uint32(b.data.Len()))

	n := binary.PutUvarint(b.temp, uint64(len(minKey)))
	n += binary.PutUvarint(b.temp[n:], uint64(blockOffset))
	n += binary.PutUvarint(b.temp[n:], uint64(blockLen))
	if _, err := b.data.Write(b.temp[:n]); err != nil {
		return err
	}
	if _, err := b.data.Write(minKey); err != nil {
		return err
	}
	return nil
}

func (b *BlockBuilder) estimateSize() int {
	return b.data.Len() + 4*len(b.offsets) + 4
}

func (b *BlockBuilder) build() *Block {
	data := make([]byte, b.data.Len())
	offset := make([]uint32, len(b.offsets))
	copy(data, b.data.Bytes())
	copy(offset, b.offsets)

	return &Block{data, offset}
}

func (b *BlockBuilder) reset() {
	b.data.Reset()
	b.offsets = b.offsets[:0]
}

/*
block format:

	| len(key1) | len(val1) | key1 | val1 | ... | key1 offset | key2 offset | ... | num of key |
*/
type Block struct {
	data   []byte
	offset []uint32
}

func EncodeBlock(b *Block) []byte {
	// TODO: reuse slice
	sizeBuf := make([]byte, 4*len(b.offset)+4)
	for i, off := range b.offset {
		binary.BigEndian.PutUint32(sizeBuf[4*i:], off)
	}
	binary.BigEndian.PutUint32(sizeBuf[4*len(b.offset):], uint32(len(b.offset)))

	b.data = append(b.data, sizeBuf...)
	return b.data
}

func DecodeBlock(data []byte) *Block {
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

func (b *Block) entry(i int) (key, val []byte, ok bool) {
	if i >= len(b.offset) || i < 0 {
		return nil, nil, false
	}
	idx := int(b.offset[i])
	keyLen, n1 := binary.Uvarint(b.data[idx:])
	valLen, n2 := binary.Uvarint(b.data[idx+n1:])
	idx = idx + n1 + n2
	return b.data[idx : idx+int(keyLen)], b.data[idx+int(keyLen) : idx+int(keyLen)+int(valLen)], true
}

// seek binary search for greater or equal key, and return its idx; return -1 if not found
func (b *Block) seek(cmp compare.Comparator, key []byte) int {
	f := func(i int) bool {
		ikey, _, _ := b.entry(i)
		return cmp.Compare(ikey, key) < 0
	}

	low, high := 0, len(b.offset)-1
	for low < high {
		mid := (low + high) >> 1
		if f(mid) {
			low = mid + 1
		} else {
			high = mid
		}
	}

	fkey, _, _ := b.entry(low)
	if cmp.Compare(fkey, key) < 0 {
		return -1
	}
	return low
}

// TODO: check necessity
// func (b *Block) get(key []byte) ([]byte, bool) {
// 	idx := b.seek(key)
// 	ekey, val, _ := b.entry(idx)
// 	if !bytes.Equal(ekey, key) {
// 		return nil, false
// 	}
// 	return val, true
// }

var _ iterator.Iterator = (*BlockIterator)(nil)

type BlockIterator struct {
	cmp compare.Comparator

	block  *Block
	curIdx int

	key, val []byte
}

func NewBlockIterator(cmp compare.Comparator, b *Block) *BlockIterator {
	iter := &BlockIterator{
		cmp:    cmp,
		block:  b,
		curIdx: 0,
	}
	iter.First()
	return iter
}

func (i *BlockIterator) First() {
	i.curIdx = 0
	i.key, i.val, _ = i.block.entry(0)
}

// Next set key, val to nil if hit endpoint
func (i *BlockIterator) Next() {
	i.curIdx += 1
	i.key, i.val, _ = i.block.entry(i.curIdx)
}

func (i *BlockIterator) Prev() {
	// TODO
}

func (i *BlockIterator) Key() []byte {
	return i.key
}

func (i *BlockIterator) Value() []byte {
	return i.val
}

func (i *BlockIterator) Valid() bool {
	return i.key != nil
}

func (i *BlockIterator) Seek(target []byte) {
	idx := i.block.seek(i.cmp, target)
	i.key, i.val, _ = i.block.entry(idx)
	i.curIdx = idx
}

/*
index block format:

	| block1 minKey len | block1 offset | block1 len | block1 minKey | ...
	| block1 index offset | block2 index offset | ... | number of blocks
*/
type IndexBlock struct {
	*Block
}

// entry is for index block, to get i-th block's info
func (b *IndexBlock) entry(i int) (minKey []byte, blockOffset, blockLen uint64, ok bool) {
	if i < 0 || i >= len(b.offset) {
		ok = false
		return
	}
	idx := int(b.offset[i])
	keyLen, n1 := binary.Uvarint(b.data[idx:])
	blockOffset, n2 := binary.Uvarint(b.data[idx+n1:])
	blockLen, n3 := binary.Uvarint(b.data[idx+n1+n2:])

	idx = idx + n1 + n2 + n3
	minKey = b.data[idx : idx+int(keyLen)]

	ok = true
	return
}

// seek is for index block, to find the block that may contain key
func (b *IndexBlock) seek(cmp compare.Comparator, key []byte) int {
	f := func(i int) bool {
		minKey, _, _, _ := b.entry(i)
		return cmp.Compare(minKey, key) <= 0
	}

	low, high := 0, len(b.offset)-1
	for low < high {
		// add 1 to avoid infinite loop
		mid := (low + high + 1) >> 1
		if f(mid) {
			low = mid
		} else {
			high = mid - 1
		}
	}
	return low
}

type IndexBlockIterator struct {
	cmp compare.Comparator

	indexBlock *IndexBlock
}

func NewIndexBlockIterator(cmp compare.Comparator, block *IndexBlock) *IndexBlockIterator {
	return &IndexBlockIterator{cmp, block}
}

func (i *IndexBlockIterator) First() {

}

func (i *IndexBlockIterator) Next() {

}

func (i *IndexBlockIterator) Prev() {

}

func (i *IndexBlockIterator) Seek(key []byte) {

}

func (i *IndexBlockIterator) Valid() bool {

}

func (i *IndexBlockIterator) Key() []byte {

}

func (i *IndexBlockIterator) Value() []byte {

}
