package cache

import (
	"sync"
)

type Cache interface {
	Get(key uint64, fetchFunc func() (interface{}, int64)) interface{}
	Remove(key uint64)
}

type node struct {
	key uint64
	val interface{}

	size int64

	next, prev *node
}

type lru struct {
	mu       sync.Mutex
	size     int64
	capacity int64

	head *node
}

func newLru() *lru {
	head := &node{}
	head.next = head
	head.prev = head

	return &lru{
		size:     0,
		capacity: 0,
		head:     head,
	}
}

func (l *lru) remove(n *node) {
	if n.next != nil {
		n.next.prev = n.prev
		n.prev.next = n.next
		n.prev = nil
		n.next = nil

		l.size -= n.size
	}
}

func (l *lru) insert(n *node) {
	n.next = l.head.next
	n.prev = l.head
	l.head.next.prev = n
	l.head.next = n

	l.size += n.size
}

func (l *lru) MoveToHead(n *node) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.remove(n)
	l.insert(n)
}

func (l *lru) Back() *node {
	n := l.head.prev
	if n == l.head {
		return nil
	}
	return n
}

func (l *lru) RemoveNode(n *node) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.remove(n)
}

type LRUCache struct {
	size, capacity int64

	list  *lru
	table map[uint64]*node

	mu sync.RWMutex
}

func NewLRUCache(capacity int64) *LRUCache {
	cache := &LRUCache{
		size:     0,
		capacity: capacity,
		list:     newLru(),
		table:    make(map[uint64]*node),
	}
	return cache
}

func (c *LRUCache) Get(key uint64, fetchFunc func() (interface{}, int64)) interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	n, ok := c.table[key]
	if !ok {
		n := &node{}
		n.val, n.size = fetchFunc()

		c.mu.Lock()
		c.table[key] = n
		c.size += n.size

		for c.size > c.capacity {
			back := c.list.Back()
			c.list.RemoveNode(back)
			delete(c.table, back.key)
			c.size -= back.size
		}

		c.mu.Unlock()
	}
	c.list.MoveToHead(n)

	return n.val
}

func (c *LRUCache) Remove(key uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	n, ok := c.table[key]
	if ok {
		c.mu.Lock()
		delete(c.table, n.key)
		c.size -= n.size
		c.mu.Unlock()

		c.list.RemoveNode(n)
	}
}

type NamespaceCache struct {
	cache     Cache
	namespace uint64
}

func NewNamespaceCache(c Cache, namespace uint64) *NamespaceCache {
	return &NamespaceCache{c, namespace}
}

func (n *NamespaceCache) applyNamespace(key uint64) uint64 {
	return n.namespace<<25 + key
}

func (n *NamespaceCache) Get(key uint64, fetchFunc func() (interface{}, int64)) interface{} {
	key = n.applyNamespace(key)
	return n.cache.Get(key, fetchFunc)
}

func (n *NamespaceCache) Remove(key uint64) {
	key = n.applyNamespace(key)
	n.cache.Remove(key)
}

// TODO
type SharedLRUCache struct {
}
