package cache

import (
	"testing"
)

func TestCacheGet(t *testing.T) {
	lruCache := NewLRUCache(10)

	for i := 0; i < 10; i += 1 {
		lruCache.Get(uint64(i), func() (interface{}, int64) {
			return string('a' + rune(i)), 1
		})
	}
	for i := 0; i < 10; i += 1 {
		val := lruCache.Get(uint64(i), func() (interface{}, int64) {
			t.Errorf("expect not to execute fetchFunc")
			return nil, 0
		})
		if v, ok := val.(string); !ok || v != string('a'+rune(i)) {
			t.Errorf("unexpected val: %v", v)
		}
	}

	for i := 10; i < 15; i += 1 {
		lruCache.Get(uint64(i), func() (interface{}, int64) {
			return nil, 1
		})
	}

	order := make([]int, 0)
	for i := 0; i < 10; i += 1 {
		lruCache.Get(uint64(i), func() (interface{}, int64) {
			order = append(order, i)
			return nil, 1
		})
		if len(order) != i+1 || order[i] != i {
			t.Error("expect to execute fetchFunc")
		}
	}
}

func TestCacheRemove(t *testing.T) {
	lruCache := NewLRUCache(10)

	for i := uint64(0); i < 10; i += 1 {
		lruCache.Get(i, func() (interface{}, int64) {
			return nil, 1
		})
	}
	lruCache.Remove(uint64(3))
	lruCache.Remove(uint64(4))
	lruCache.Remove(uint64(5))

	count := 0
	for i := uint64(0); i < 10; i += 1 {
		lruCache.Get(i, func() (interface{}, int64) {
			count += 1
			return nil, 1
		})
	}
	if count != 3 {
		t.Error("expect fetchFunc to be executed 3 times")
	}
}
