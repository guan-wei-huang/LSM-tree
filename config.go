package lsm

const (
	KB = 1024
	MB = 1024 * KB
)

const (
	DefaultBlockSize    = 4 * KB
	DefaultMemtableSize = 4 * MB
)

type FileType int

const (
	SstableFile FileType = iota
	LogFile
)

type Config struct {
}
