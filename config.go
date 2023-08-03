package lsm

import "lsm/compare"

const (
	KB = 1024
	MB = 1024 * KB
)

const (
	DefaultBlockSize    = 4 * KB
	DefaultMemtableSize = 2 * MB

	Level0FileNumber = 5
	FileSize         = 2 * MB
	Level1FilesSize  = 10 * MB

	FileCacheCapacity  = 500
	BlockCacheCapacity = 8 * MB
)

type FileType int

const (
	SstableFile FileType = iota
	LogFile
)

type Config struct {
}

var DefaultComparator = compare.BasicComparator{}
