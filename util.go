package lsm

import "os"

func OpenFile(path string, readOnly bool) (*os.File, error) {
	flag := os.O_CREATE | os.O_RDONLY
	if !readOnly {
		flag |= os.O_RDWR
	}
	file, err := os.OpenFile(path, flag, 0640)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func CreateFile(path string) {

}
