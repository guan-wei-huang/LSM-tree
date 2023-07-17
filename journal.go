package lsm

import (
	"encoding/binary"
	"fmt"
	"io"
)

type WriteOperation int

const (
	WriteOperationPut WriteOperation = iota
	WriteOperationDelete
)

func encodeWriteData(wop WriteOperation, data ...[]byte) []byte {
	if (wop == WriteOperationPut && len(data) != 2) || (wop == WriteOperationDelete && len(data) != 1) {
		panic("error encode write operate")
	}

	// fix: fail if len of data bigger than uint64
	size := 1 + 4 + len(data[0])
	if wop == WriteOperationPut {
		size += 4 + len(data[1])
	}
	b := make([]byte, size)
	b[0] = byte(wop)
	n := binary.PutUvarint(b[1:], uint64(len(data[0])))
	copy(b[1+n:], data[0])

	if wop == WriteOperationPut {
		n += 1 + len(data[0])
		n += binary.PutUvarint(b[n:], uint64(len(data[1])))
		copy(b[n:], data[1])
	}
	return b
}

type journal struct {
	w io.WriteCloser
}

func NewJournal(id uint64) *journal {
	j := &journal{}
	j.Reset(id)

	return j
}

func (j *journal) Write(data []byte) {
	j.w.Write(data)
}

func (j *journal) Finish() {
	j.w.Close()
}

func (j *journal) Reset(id uint64) {
	f, err := openFile(fileName(LogFile, id), false)
	if err != nil {
		panic(fmt.Sprintf("create log file err: %v", err))
	}

	j.w = f
}
