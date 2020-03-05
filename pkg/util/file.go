package util

import (
	"fmt"
	"os"
)

func CreateFile(path string) *os.File {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("failed to create file %s, error %v", path, err)
		os.Exit(1)
	}
	return f
}
