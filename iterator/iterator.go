package iterator

type Iterator interface {
	First()
	Next() bool
	Valid() bool
	Key() []byte
	Value() []byte
}
