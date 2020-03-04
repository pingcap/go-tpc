package util

import (
	"os"
	"sync"
)

func CreateFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}
	return f, nil
}

type Flock struct {
	*os.File
	*sync.Mutex
}
