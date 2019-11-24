package dbgen

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	initDriver(1)
	os.Exit(m.Run())
}

//func TestGenTable(t *testing.T) {
//	genTable(ORDER, 1, 10)
//}
