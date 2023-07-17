package iterator

import (
	"container/heap"
	"lsm/compare"
)

type Iterator interface {
	First()
	Next()
	Prev()
	Seek(key []byte)
	Valid() bool
	Key() []byte
	Value() []byte
}

type MergeIterator struct {
	cmp compare.Comparator

	iters []Iterator
}

func NewMergeIterator(iters []Iterator, cmp compare.Comparator) *MergeIterator {
	return &MergeIterator{
		cmp:   cmp,
		iters: iters,
	}
}

func (m MergeIterator) Len() int {
	return len(m.iters)
}

func (m MergeIterator) Less(i, j int) bool {
	if !m.iters[i].Valid() {
		return false
	} else if !m.iters[j].Valid() {
		return true
	}

	return m.cmp.Compare(m.iters[i].Key(), m.iters[j].Key()) < 0
}

func (m MergeIterator) Swap(i, j int) {
	m.iters[i], m.iters[j] = m.iters[j], m.iters[i]
}

func (m MergeIterator) Push(iter interface{}) {
	m.iters[len(m.iters)-1] = iter.(Iterator)
}

func (m MergeIterator) Pop() interface{} {
	if !m.Valid() {
		return nil
	}
	return m.iters[len(m.iters)-1]
}

func (m MergeIterator) Next() {
	iter := heap.Pop(m).(Iterator)
	iter.Next()
	heap.Push(m, iter)
}

func (m MergeIterator) First() {
	for _, iter := range m.iters {
		iter.First()
	}
	heap.Init(m)
}

func (m MergeIterator) Prev() {
	// TODO
}

func (m MergeIterator) Seek(key []byte) {
	for _, iter := range m.iters {
		iter.Seek(key)
	}
	heap.Init(m)
}

func (m MergeIterator) Valid() bool {
	return m.iters[0].Valid()
}

func (m MergeIterator) Key() []byte {
	return m.iters[0].Key()
}

func (m MergeIterator) Value() []byte {
	return m.iters[0].Value()
}
