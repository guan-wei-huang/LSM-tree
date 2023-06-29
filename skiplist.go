package lsm

import (
	"bytes"
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
		forward: make([]*Node, height),
	}
}

type SkipList struct {
	maxHeight uint
	curHeight uint
	prob      float32

	head *Node
}

type SkipListIter struct {
	node *Node
}

func NewSkiplist() *SkipList {
	list := SkipList{
		maxHeight: 12,
		prob:      0.5,
		head:      NewNode(nil, nil, 12),
	}
	return &list
}

func (l *SkipList) Insert(key, val []byte) {
	node := l.head
	prev := make([]*Node, l.maxHeight)

	for i := int(l.curHeight); i >= 0; i-- {
		forwardNode := node.forward[i]
		for forwardNode == nil || bytes.Compare(forwardNode.key, key) < 0 {
			node = forwardNode
		}
		prev[i] = node
	}

	height := l.randomHeight()
	newNode := NewNode(key, val, height)
	if height > l.curHeight {
		for i := l.curHeight + 1; i <= height; i++ {
			prev[i] = l.head
		}
		l.curHeight = height
	}

	for i := 0; i <= int(l.curHeight); i++ {
		newNode.forward[i] = prev[i].forward[i]
		prev[i].forward[i] = newNode
	}
}

func (l *SkipList) NewIterator() *SkipListIter {
	return &SkipListIter{
		l.head.forward[0],
	}
}

func (l *SkipList) randomHeight() uint {
	var height uint = 1
	for height < l.maxHeight && rand.Float32() >= l.prob {
		height++
	}
	return height
}

func (it *SkipListIter) Key() []byte {
	return it.node.key
}

func (it *SkipListIter) Value() []byte {
	return it.node.val
}

func (it *SkipListIter) Next() bool {
	if it.node == nil {
		return false
	}
	it.node = it.node.forward[0]
	return true
}

func (it *SkipListIter) Valid() bool {
	return it.node != nil
}
