package lsm

import (
	"bytes"
	"lsm/compare"
	"math/rand"
)

type Node struct {
	key []byte
	val []byte

	forward []*Node
}

func NewNode(key, val []byte, height uint) *Node {
	return &Node{
		key:     key,
		val:     val,
		forward: make([]*Node, height+1),
	}
}

type SkipList struct {
	maxHeight uint
	curHeight uint
	prob      float32

	head *Node

	cmp compare.Comparator
}

func NewSkiplist(cmp compare.Comparator) *SkipList {
	list := SkipList{
		maxHeight: 12,
		curHeight: 0,
		prob:      0.25,
		head:      NewNode(nil, nil, 12),

		cmp: cmp,
	}
	return &list
}

func (l *SkipList) findGreaterOrEqual(key []byte, prev []*Node) *Node {
	node := l.head
	for i := int(l.curHeight); i >= 0; i-- {
		for node.forward[i] != nil && bytes.Compare(node.forward[i].key, key) < 0 {
			node = node.forward[i]
		}
		if prev != nil {
			prev[i] = node
		}
	}
	return node.forward[0]
}

func (l *SkipList) Get(key []byte) (val []byte, exist bool) {
	node := l.findGreaterOrEqual(key, nil)
	if node != nil && l.cmp.Compare(node.key, key) == 0 {
		val = make([]byte, len(node.val))
		copy(val, node.val)
		return val, true
	}
	return nil, false
}

func (l *SkipList) Insert(key, val []byte) {
	prev := make([]*Node, l.maxHeight)
	l.findGreaterOrEqual(key, prev)

	height := l.randomHeight()
	newNode := NewNode(key, val, height)
	if height > l.curHeight {
		for i := l.curHeight + 1; i <= height; i++ {
			prev[i] = l.head
		}
		l.curHeight = height
	}

	for i := 0; i <= int(height); i++ {
		newNode.forward[i] = prev[i].forward[i]
		prev[i].forward[i] = newNode
	}
}

func (l *SkipList) randomHeight() uint {
	var height uint = 1
	for height < l.maxHeight && rand.Float32() < l.prob {
		height++
	}
	return height
}

type SkipListIter struct {
	list *SkipList
	node *Node
}

func NewSkiplistIterator(list *SkipList) *SkipListIter {
	return &SkipListIter{
		list: list,
		node: list.head.forward[0],
	}
}

func (i *SkipListIter) First() {
	i.node = i.list.head.forward[0]
}

func (i *SkipListIter) Key() []byte {
	return i.node.key
}

func (i *SkipListIter) Value() []byte {
	return i.node.val
}

func (i *SkipListIter) Next() {
	if i.node != nil {
		i.node = i.node.forward[0]
	}
}

func (i *SkipListIter) Prev() {
	// TODO
}

func (i *SkipListIter) Seek(key []byte) {
	n := i.list.findGreaterOrEqual(key, nil)
	i.node = n
}

func (i *SkipListIter) Valid() bool {
	return i.node != nil
}
