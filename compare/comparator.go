package compare

import "bytes"

type Comparator interface {
	Compare(a, b []byte) int
}

type BasicComparator struct{}

func (c BasicComparator) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}
