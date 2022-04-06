package sink

import (
	"reflect"
)

var (
	typeUInt64  = reflect.TypeOf((*uint64)(nil)).Elem()
	typeInt64   = reflect.TypeOf((*int64)(nil)).Elem()
	typeFloat64 = reflect.TypeOf((*float64)(nil)).Elem()
	typeString  = reflect.TypeOf((*string)(nil)).Elem()
)
