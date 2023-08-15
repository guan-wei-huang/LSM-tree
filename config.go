package lsm

import "lsm/compare"

const (
	KB = 1024
	MB = 1024 * KB
)

var (
	DefaultBlockSize    = 4 * KB
	DefaultMemtableSize = 2 * MB

	Level0FileNumber = 4
	FileSize         = 2 * MB
	Level1FilesSize  = 10 * MB
	SizeMultiplier   = 10
	MaximumLevel     = 10

	FileCacheCapacity  = 500
	BlockCacheCapacity = 8 * MB

	DirectoryPath = "./lsm"

	DefaultComparator = compare.BasicComparator{}
)

type FileType int

const (
	SstableFile FileType = iota
	LogFile
)

type Config struct {
	// TODO: customize config
}
