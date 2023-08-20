package sstable

import (
	"bytes"
	"encoding/binary"
	"log"
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

type FilterBuilder struct {
	bloom *bloomFilterGenerator
	buf   *bytes.Buffer

	offsets []uint32
}

func NewFilterBuilder() *FilterBuilder {
	buf := make([]byte, 0)
	return &FilterBuilder{
		bloom: NewBloomFilterGenerator(),
		buf:   bytes.NewBuffer(buf),
	}
}

func (f *FilterBuilder) addKey(key []byte) {
	f.bloom.add(key)
}

func (f *FilterBuilder) finish() {
	f.offsets = append(f.offsets, uint32(f.buf.Len()))

	filterEntry := f.bloom.build()
	if _, err := f.buf.Write(filterEntry); err != nil {
		log.Printf("lsm-tree: read block failed: %v", err)
		return
	}
}

func (f *FilterBuilder) build() *Block {
	return &Block{
		data:   f.buf.Bytes(),
		offset: f.offsets,
	}
}

/*
block format:

	| len(key1) | len(val1) | key1 | val1 | ... | key1 offset | key2 offset | ... | num of key |
*/
type Block struct {
	data   []byte
	offset []uint32
}

func (b *Block) numEntries() int {
	return len(b.offset)
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

func (b *Block) get(cmp compare.Comparator, key []byte) ([]byte, bool) {
	idx := b.seek(cmp, key)
	ekey, val, _ := b.entry(idx)
	if cmp.Compare(ekey, key) != 0 {
		return nil, false
	}
	return val, true
}

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

// entry get i'th entry's content, desc: i'th block offset & len
func (b *IndexBlock) entry(i int) (desc []byte, minKey []byte) {
	if i < 0 || i >= len(b.offset) {
		return nil, nil
	}

	idx := int(b.offset[i])
	keyLen, n1 := binary.Uvarint(b.data[idx:])
	_, n2 := binary.Uvarint(b.data[idx+n1:])
	_, n3 := binary.Uvarint(b.data[idx+n1+n2:])

	keyIdx := idx + n1 + n2 + n3
	return b.data[idx+n1 : keyIdx], b.data[keyIdx : keyIdx+int(keyLen)]
}

// find out which block the key might fall in and return its index
func (b *IndexBlock) seek(cmp compare.Comparator, key []byte) int {
	f := func(i int) bool {
		_, minKey := b.entry(i)
		return cmp.Compare(minKey, key) <= 0
	}

	_, minKey := b.entry(0)
	if cmp.Compare(key, minKey) < 0 {
		return -1
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

var _ iterator.IndexIterator = (*IndexBlockIterator)(nil)

type IndexBlockIterator struct {
	reader *TableReader

	indexBlock *IndexBlock

	curIdx int

	key, val []byte
}

func NewIndexBlockIterator(reader *TableReader, block *IndexBlock) *IndexBlockIterator {
	i := &IndexBlockIterator{
		reader:     reader,
		indexBlock: block,
	}
	i.First()
	return i
}

func (i *IndexBlockIterator) First() {
	i.curIdx = 0
	i.key, i.val = i.indexBlock.entry(0)
}

func (i *IndexBlockIterator) Next() {
	i.curIdx = min(i.curIdx+1, i.indexBlock.numEntries())
	i.key, i.val = i.indexBlock.entry(i.curIdx)
}

func (i *IndexBlockIterator) Prev() {
	i.curIdx = max(i.curIdx-1, -1)
	i.key, i.val = i.indexBlock.entry(i.curIdx)
}

func (i *IndexBlockIterator) Seek(key []byte) {
	idx := i.indexBlock.seek(i.reader.cmp, key)
	i.curIdx = idx
	i.key, i.val = i.indexBlock.entry(idx)
}

func (i *IndexBlockIterator) Valid() bool {
	return i.curIdx < i.indexBlock.numEntries() && i.curIdx >= 0
}

func (i *IndexBlockIterator) Key() []byte {
	return i.key
}

func (i *IndexBlockIterator) Value() []byte {
	return i.val
}

func (i *IndexBlockIterator) Get() iterator.Iterator {
	if !i.Valid() {
		return nil
	}

	offset, size := decodeIndexEntry(i.key)
	b, err := i.reader.readBlock(offset, size)
	if err != nil {
		log.Printf("lsm-tree: read block failed: %v", err)
		return nil
	}
	return NewBlockIterator(i.reader.cmp, b)
}

type FilterBlock struct {
	block *Block
	bf    bloomFilter
}

func (f *FilterBlock) contain(index int, key []byte) bool {
	if index < 0 || index >= f.block.numEntries() {
		return false
	}

	offset, size := f.block.offset[index], uint32(0)
	if index == f.block.numEntries()-1 {
		size = uint32(len(f.block.data)) - offset
	} else {
		size = f.block.offset[index+1] - f.block.offset[index]
	}
	return f.bf.contain(f.block.data[offset:offset+size], key)
}
