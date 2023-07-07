package iterator

type Iterator interface {
	First()
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
}
