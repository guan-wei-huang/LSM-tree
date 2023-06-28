package lsm

import (
	"lsm/compare"
	"math/rand"
)

type Node[T compare.Comparable] struct {
	val     *T
	forward []*Node[T]
}

func NewNode[T compare.Comparable](val *T, height uint) *Node[T] {
	return &Node[T]{
		val:     val,
		forward: make([]*Node[T], height),
	}
}

type SkipList[T compare.Comparable] struct {
	maxHeight uint
	curHeight uint
	prob      float32

	head *Node[T]
}

func NewSkiplist[T compare.Comparable]() *SkipList[T] {
	list := SkipList[T]{
		maxHeight: 12,
		prob:      0.5,
		head:      NewNode[T](nil, 12),
	}
	return &list
}

func (l *SkipList[T]) Insert(val T) {
	node := l.head
	prev := make([]*Node[T], l.maxHeight)

	for i := int(l.curHeight); i >= 0; i-- {
		forwardNode := node.forward[i]
		for forwardNode == nil || (*forwardNode.val).Compare(val.ToByte()) < 0 {
			node = forwardNode
		}
		prev[i] = node
	}

	height := l.randomHeight()
	newNode := NewNode[T](&val, height)
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

func (l *SkipList[T]) randomHeight() uint {
	var height uint = 1
	for height < l.maxHeight && rand.Float32() >= l.prob {
		height++
	}
	return height
}
