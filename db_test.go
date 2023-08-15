package lsm

import (
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	exitCode := m.Run()
	if exitCode == 0 {
		if _, err := os.Lstat(DirectoryPath); err == nil {
			if err := os.RemoveAll(DirectoryPath); err != nil {
				log.Println(err)
			}
		}
	}
	os.Exit(exitCode)
}

type testDB struct {
	db      *DB
	storage *Storage
	t       *testing.T
}

func newTestDB(t *testing.T) *testDB {
	db := New()
	return &testDB{
		db:      db,
		storage: db.storage,
		t:       t,
	}
}

func (d *testDB) put(key, val string) {
	d.db.Put([]byte(key), []byte(val))
}

func (d *testDB) get(key, expect string) {
	val := d.db.Get([]byte(key))
	if string(val) != expect {
		d.t.Errorf("invalid value, key: %v, expect val: %v, got val: %v", key, expect, val)
	}
}

func (d *testDB) bulkPut(size int) (num int) {
	return d.bulkPutFrom(size, 0)
}

// each key-value is 100 bytes
func (d *testDB) bulkPutFrom(size int, from int) (num int) {
	num = (size + 99) / 100
	for i := from; i < from+num; i += 1 {
		key, val := getKV(i)
		d.put(key, val)
	}
	return num
}

func (d *testDB) pauseCompactGoroutine() {
	d.db.pauseChan <- struct{}{}
}

func (d *testDB) memCompaction() {
	d.db.frozenMem()
	d.db.newMem()
	d.db.memCompaction()
}

func (d *testDB) assertLevelFilesNum(nums ...int) {
	for i, num := range nums {
		assert.Equal(d.t, num, d.storage.numTables(i))
	}
}

func getKV(num int) (string, string) {
	key := fmt.Sprintf("%010d", num)
	val := strings.Repeat(string('a'+rune(num%26)), 90)
	return key, val
}

func TestDB_ReadWrite(t *testing.T) {
	d := newTestDB(t)
	d.put("k1", "v1")
	d.put("k2", "v2")
	d.put("k3", "v3")
	d.get("k1", "v1")
	d.get("k2", "v2")
	d.get("k3", "v3")
}

func TestDB_NotFound(t *testing.T) {
	d := newTestDB(t)
	d.put("k1", "v1")
	d.get("k2", "")
	d.get("k3", "")
}

func TestDB_Replicate(t *testing.T) {
	d := newTestDB(t)
	d.put("k1", "v1")
	d.get("k1", "v1")

	d.put("k1", "v2")
	d.get("k1", "v2")
}

func TestDB_FrozenMemtable(t *testing.T) {
	d := newTestDB(t)
	d.pauseCompactGoroutine()

	mtable := d.db.mtable
	count := d.bulkPut(2 * MB) // fill memtable

	assert.NotNil(t, d.db.mtable)
	assert.NotNil(t, d.db.immtable)
	assert.Same(t, mtable, d.db.immtable)
	assert.NotSame(t, mtable, d.db.mtable)

	// test get val from immtable
	for i := 0; i < count; i++ {
		key, val := getKV(i)
		d.get(key, val)
	}

	key := "nonexistent"
	val := ""
	d.get(key, val)

	key = fmt.Sprintf("%010d", 1)
	val = "replace kv-pair in immtable"
	d.put(key, val)
	d.get(key, val)
}

func TestDB_MemCompaction(t *testing.T) {
	d := newTestDB(t)
	d.pauseCompactGoroutine()

	nRec := 0
	for i := 0; i < 3; i++ {
		count := d.bulkPutFrom(1*MB, nRec)
		d.memCompaction()

		_, imm := d.db.getMemTables(true)
		assert.Nil(t, imm)

		num := d.storage.numTables(0)
		assert.Equal(t, i+1, num)

		tInfo := d.storage.level0[num-1]
		minKey, _ := getKV(nRec)
		maxKey, _ := getKV(nRec + count - 1)
		assert.Equal(t, minKey, string(tInfo.minKey))
		assert.Equal(t, maxKey, string(tInfo.maxKey))
		assert.GreaterOrEqual(t, tInfo.size, uint64(1*MB))

		nRec += count
	}
}

func TestDB_TriggerLevel0Compaction(t *testing.T) {
	d := newTestDB(t)
	d.pauseCompactGoroutine()

	nRec := 0
	for i := 1; i <= Level0FileNumber+1; i++ {
		nRec += d.bulkPutFrom(1*KB, nRec)
		d.memCompaction()
	}

	select {
	case cRange := <-d.db.levelCompact:
		assert.Equal(t, 0, cRange.level)
	default:
		t.Error("expected to receive level 0, but no message received")
	}
}

func TestDB_MajorCompaction(t *testing.T) {
	Level1FilesSize = 3 * KB
	SizeMultiplier = 2

	d := newTestDB(t)

	nRec := 0
	// insert 2.5 KB
	for i := 1; i <= Level0FileNumber+1; i++ {
		nRec += d.bulkPutFrom(0.5*KB, nRec)
		d.memCompaction()
	}

	t.Log("wait for major compaction")
	time.Sleep(1 * time.Second)

	d.assertLevelFilesNum(0, 1)

	for i := 0; i < nRec; i++ {
		key, val := getKV(i)
		d.get(key, val)
	}

	for i := 1; i <= 5; i++ {
		nRec += d.bulkPutFrom(0.5*KB, nRec)
		d.memCompaction()
	}

	t.Log("wait for major compaction")
	time.Sleep(1 * time.Second)

	d.assertLevelFilesNum(0, 1, 1)

	for i := 0; i < nRec; i++ {
		key, val := getKV(i)
		d.get(key, val)
	}
}
