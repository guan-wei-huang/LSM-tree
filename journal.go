package lsm

import (
	"encoding/binary"
	"io"
)

type WriteOperation uint8

const (
	WriteOperationPut WriteOperation = iota
	WriteOperationDelete
)

type journal struct {
	w io.WriteCloser

	buf [1 + binary.MaxVarintLen64*2]byte
}

func NewJournal(w io.WriteCloser) *journal {
	j := &journal{}
	j.Reset(w)

	return j
}

func (j *journal) Write(data []byte) {
	j.w.Write(data)
}

func (j *journal) WriteRecord(wop WriteOperation, data ...[]byte) {
	enc := j.encodeWriteRecord(wop, data...)
	j.Write(enc)
}

func (j *journal) Finish() {
	j.w.Close()
}

func (j *journal) Reset(w io.WriteCloser) {
	j.w = w
}

/*
format:

	| Put (1 byte) | len of key | len of value | key | value |
	or
	| Delete (1 byte) | len of key | key |
*/
func (j *journal) encodeWriteRecord(wop WriteOperation, data ...[]byte) []byte {
	if (wop == WriteOperationPut && len(data) != 2) || (wop == WriteOperationDelete && len(data) != 1) {
		panic("error encode write operate")
	}

	j.buf[0] = byte(wop)
	prefix := 1
	prefix += binary.PutUvarint(j.buf[1:], uint64(len(data[0])))
	if wop == WriteOperationPut {
		prefix += binary.PutUvarint(j.buf[prefix:], uint64(len(data[1])))
	}

	size := prefix + len(data[0])
	if wop == WriteOperationPut {
		size += len(data[1])
	}

	b := make([]byte, size)
	copy(b[:prefix], j.buf[:prefix])
	copy(b[prefix:], data[0])
	if wop == WriteOperationPut {
		copy(b[prefix+len(data[0]):], data[1])
	}
	return b
}
