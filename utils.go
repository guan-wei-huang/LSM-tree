package lsm

import (
	"fmt"
	"os"
	"time"
)

func createDir() error {
	DirectoryPath = fmt.Sprintf("./lsm/%v", time.Now())
	return os.MkdirAll(DirectoryPath, 0777)
}

// create file if doesnt exist
func openFile(fname string, readOnly bool) (*os.File, error) {
	flag := os.O_CREATE | os.O_RDONLY
	if !readOnly {
		flag |= os.O_RDWR
	}
	file, err := os.OpenFile(fname, flag, 0640)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func fileName(ftype FileType, id uint64) string {
	if ftype == SstableFile {
		return fmt.Sprintf("%v/sst-%v.ldb", DirectoryPath, id)
	} else if ftype == LogFile {
		return fmt.Sprintf("%v/log-%v.log", DirectoryPath, id)
	}
	return ""
}

func removeFile(fname string) error {
	return os.Remove(fname)
}

func levelFilesSize(level int) uint64 {
	size := uint64(Level1FilesSize)
	for i := 2; i <= level; i += 1 {
		size *= uint64(SizeMultiplier)
	}
	return size
}
