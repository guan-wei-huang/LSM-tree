package compare

type Comparable interface {
	Compare(a []byte) int
	ToByte() []byte
}
