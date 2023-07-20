package iterator

import (
	"container/heap"
	"lsm/compare"
)

// once iterator invalid, have to use First() to reset iterator, using Next() or Prev() have no effect
type Iterator interface {
	First()
	Next()
	Prev()
	Seek(key []byte)
	Valid() bool
	Key() []byte
	Value() []byte
}

type IndexIterator interface {
	Iterator
	Get() Iterator
}

type TwoLevelIterator struct {
	IndexIterator
	Iterator
}

func NewTwoLevelIterator(idxIter IndexIterator) *TwoLevelIterator {
	twoIter := &TwoLevelIterator{
		IndexIterator: idxIter,
	}
	twoIter.First()
	return twoIter
}

func (t *TwoLevelIterator) First() {
	t.IndexIterator.First()
	t.Iterator = t.IndexIterator.Get()
}

func (t *TwoLevelIterator) Next() {
	if t.Iterator.Valid() {
		if t.Iterator.Next(); !t.Iterator.Valid() {
			if t.IndexIterator.Next(); t.IndexIterator.Valid() {
				t.Iterator = t.IndexIterator.Get()
			}
		}
	}
}

func (t *TwoLevelIterator) Prev() {
	// TODO
}

func (t *TwoLevelIterator) Seek(key []byte) {
	t.IndexIterator.Seek(key)
	t.Iterator = t.IndexIterator.Get()
	t.Iterator.Seek(key)
}

func (t *TwoLevelIterator) Valid() bool {
	return t.Iterator.Valid()
}

func (t *TwoLevelIterator) Key() []byte {
	return t.Iterator.Key()
}

func (t *TwoLevelIterator) Value() []byte {
	return t.Iterator.Value()
}

type MergeIterator struct {
	cmp compare.Comparator

	idx []int

	iters []Iterator
}

func NewMergeIterator(iters []Iterator, cmp compare.Comparator) *MergeIterator {
	m := &MergeIterator{
		cmp:   cmp,
		iters: iters,
		idx:   make([]int, len(iters)),
	}
	for i := range iters {
		m.idx[i] = i
	}
	m.First()

	return m
}

func (m *MergeIterator) Len() int {
	return len(m.iters)
}

func (m *MergeIterator) Less(i, j int) bool {
	i, j = m.idx[i], m.idx[j]

	if !m.iters[i].Valid() {
		return false
	} else if !m.iters[i].Valid() {
		return true
	}

	if r := m.cmp.Compare(m.iters[i].Key(), m.iters[j].Key()); r != 0 {
		return r < 0
	}
	return i < j
}

func (m *MergeIterator) Swap(i, j int) {
	m.idx[i], m.idx[j] = m.idx[j], m.idx[i]
}

func (m *MergeIterator) Push(itIdx interface{}) {
	m.idx = append(m.idx, itIdx.(int))
}

func (m *MergeIterator) Pop() interface{} {
	i := m.idx[len(m.idx)-1]
	m.idx = m.idx[:len(m.idx)-1]
	return i
}

func (m *MergeIterator) Next() {
	if !m.Valid() {
		return
	}
	key := m.Key()
	for m.Valid() && m.cmp.Compare(key, m.Key()) == 0 {
		idx := heap.Pop(m).(int)
		iter := m.iters[idx]
		if iter.Next(); iter.Valid() {
			heap.Push(m, idx)
		}
	}
}

func (m *MergeIterator) First() {
	m.idx = m.idx[:0]
	for i, iter := range m.iters {
		iter.First()
		m.idx = append(m.idx, i)
	}
	heap.Init(m)
}

func (m *MergeIterator) Prev() {
	// TODO
}

func (m *MergeIterator) Seek(key []byte) {
	m.idx = m.idx[:0]
	for i, iter := range m.iters {
		iter.Seek(key)
		if iter.Valid() {
			m.idx = append(m.idx, i)
		}
	}

	heap.Init(m)
}

func (m *MergeIterator) Valid() bool {
	return m.Len() > 0
}

func (m *MergeIterator) Key() []byte {
	if !m.Valid() {
		return nil
	}
	front := m.idx[0]
	return m.iters[front].Key()
}

func (m *MergeIterator) Value() []byte {
	if !m.Valid() {
		return nil
	}
	front := m.idx[0]
	return m.iters[front].Value()
}
