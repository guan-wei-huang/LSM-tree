package lsm

import (
	"bytes"
	"strings"
	"testing"
)

func encodeWriteRecordStr(j *journal, wop WriteOperation, data ...string) []byte {
	dataBytes := make([][]byte, 0, len(data))
	for _, d := range data {
		dataBytes = append(dataBytes, []byte(d))
	}
	return j.encodeWriteRecord(wop, dataBytes...)
}

func TestEncodeWriteRecord(t *testing.T) {
	j := NewJournal(nil)
	t.Run("panic on invalid format for put record", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expect panic, but got nil")
			}
		}()

		key := "test"
		encodeWriteRecordStr(j, WriteOperationPut, key)
	})

	t.Run("panic on invalid format for delete record", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expect panic, but got nil")
			}
		}()

		key := "test"
		val := "val"
		encodeWriteRecordStr(j, WriteOperationDelete, key, val)
	})

	t.Run("put record format", func(t *testing.T) {
		key := "test_key"
		val := "test_value"
		expectRec := []byte{0x00, 0x08, 0x0a}
		expectRec = append(expectRec, []byte(key)...)
		expectRec = append(expectRec, []byte(val)...)

		encRec := encodeWriteRecordStr(j, WriteOperationPut, key, val)
		if !bytes.Equal(expectRec, encRec) {
			t.Errorf("invalid encoded record, expect: %v, got: %v", expectRec, encRec)
		}

		key = strings.Repeat(key, 25) // len = 200
		val = strings.Repeat(val, 25) // len = 250
		expectRec = []byte{0x00, 0xc8, 0x01, 0xfa, 0x01}
		expectRec = append(expectRec, []byte(key)...)
		expectRec = append(expectRec, []byte(val)...)

		encRec = encodeWriteRecordStr(j, WriteOperationPut, key, val)
		if !bytes.Equal(expectRec, encRec) {
			t.Errorf("invalid encoded record, expect: %v, got: %v", expectRec, encRec)
		}
	})

	t.Run("delete record format", func(t *testing.T) {
		key := "test_key"
		expectRec := []byte{0x01, 0x08}
		expectRec = append(expectRec, []byte(key)...)

		encRec := encodeWriteRecordStr(j, WriteOperationDelete, key)
		if !bytes.Equal(expectRec, encRec) {
			t.Errorf("invalid encoded record, expect: %v, got: %v", expectRec, encRec)
		}
	})
}
