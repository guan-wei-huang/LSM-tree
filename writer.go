package lsm

import (
	"bytes"
	"encoding/binary"
	"os"
)

type Writer struct {
	file       *os.File
	maxBufSize int
	numEntries int

	temp []byte
	buf  *bytes.Buffer
}

func NewWriter(path string) *Writer {
	file, err := openFile(path, false)
	if err != nil {
		panic(err)
	}
	return &Writer{
		file: file,
		temp: make([]byte, 30),
		buf:  bytes.NewBuffer(make([]byte, 0)),
	}
}

func (w *Writer) Append(key, value []byte) error {

	n := binary.PutUvarint(w.temp, uint64(len(key)))
	n += binary.PutUvarint(w.temp[n:], uint64(len(value)))
	if _, err := w.buf.Write(w.temp); err != nil {
		return err
	}
	if _, err := w.buf.Write(key); err != nil {
		return err
	}
	if _, err := w.buf.Write(value); err != nil {
		return err
	}
	w.numEntries += 1

	if w.buf.Len() >= w.maxBufSize {
		if err := w.writeToFile(); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) writeToFile() error {
	if _, err := w.file.Write(w.buf.Bytes()); err != nil {
		return err
	}
	w.buf.Reset()
	w.numEntries = 0
	return nil
}
