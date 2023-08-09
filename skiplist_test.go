package lsm

import (
	"runtime"
	"testing"
)

type testList struct {
	t    *testing.T
	list *SkipList
}

func newTestList(t *testing.T) *testList {
	return &testList{
		t:    t,
		list: NewSkiplist(DefaultComparator),
	}
}

func (l *testList) insert(key, val string) {
	l.list.Insert([]byte(key), []byte(val))
}

func (l *testList) get(key, expect string) {
	val, exist := l.list.Get([]byte(key))

	if len(expect) == 0 {
		if exist {
			_, file, line, _ := runtime.Caller(1)
			l.t.Errorf("\n%v:%v: invalid value, expect nil, got: %v", file, line, val)
		}
	} else {
		if expect != string(val) {
			_, file, line, _ := runtime.Caller(1)
			l.t.Errorf("\n%v:%v: invalid value, expect: %v, got: %v", file, line, expect, val)
		}
	}
}

func TestListReadWrite(t *testing.T) {
	list := newTestList(t)
	list.insert("k1", "v1")
	list.insert("k2", "v2")
	list.insert("k3", "v3")
	list.get("k1", "v1")
	list.get("k2", "v2")
	list.get("k3", "v3")
	list.get("k4", "")
}
