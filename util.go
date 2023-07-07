package lsm

import (
	"fmt"
	"os"
)

// create file if doesnt exist
func OpenFile(fname string, readOnly bool) (*os.File, error) {
	flag := os.O_CREATE | os.O_RDONLY
	if !readOnly {
		flag |= os.O_RDWR
	}
	// TODO: file path
	file, err := os.OpenFile(path, flag, 0640)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func fileName(ftype FileType, id uint64) string {
	if ftype == SstableFile {
		return fmt.Sprintf("sst-%v.ldb", id)
	} else if ftype == LogFile {
		return fmt.Sprintf("log-%v.log", id)
	}
	return ""
}
