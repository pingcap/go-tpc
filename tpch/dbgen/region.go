package dbgen

import (
	"fmt"
	"io"
)

const (
	rCmntSd  = 42
	rCmntLen = 72
)

type Region struct {
	Code    dssHuge
	Text    string
	Join    long
	Comment string
}

type regionLoader struct {
	io.StringWriter
}

func (r regionLoader) Load(item interface{}) error {
	region := item.(*Region)
	if _, err := r.WriteString(
		fmt.Sprintf("%d|%s|%s|\n",
			region.Code,
			region.Text,
			region.Comment)); err != nil {
		return err
	}
	return nil
}

func (r regionLoader) Flush() error {
	return nil
}

func NewRegionLoader(writer io.StringWriter) regionLoader {
	return regionLoader{writer}
}

func makeRegion(idx dssHuge) *Region {
	region := &Region{}

	region.Code = idx - 1
	region.Text = regions.members[idx-1].text
	region.Join = 0
	region.Comment = makeText(rCmntLen, rCmntSd)
	return region
}
