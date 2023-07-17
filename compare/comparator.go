package compare

type Comparator interface {
	Compare(a, b []byte) int
}
