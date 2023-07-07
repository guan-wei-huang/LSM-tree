package lsm

import (
	"io"
)

type journal struct {
	w io.Writer
}

func NewJournal() *journal {
	return &journal{}
}
