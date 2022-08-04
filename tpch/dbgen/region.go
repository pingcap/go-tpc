package dbgen

import (
	"context"
	"io"

	"github.com/pingcap/go-tpc/pkg/sink"
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
	*sink.CSVSink
}

func (r regionLoader) Load(item interface{}) error {
	region := item.(*Region)
	if err := r.WriteRow(context.TODO(),
		region.Code,
		region.Text,
		region.Comment); err != nil {
		return err
	}
	return nil
}

func (r regionLoader) Flush() error {
	return r.CSVSink.Flush(context.TODO())
}

func NewRegionLoader(w io.Writer) regionLoader {
	return regionLoader{sink.NewCSVSinkWithDelimiter(w, '|')}
}

func makeRegion(idx dssHuge) *Region {
	region := &Region{}

	region.Code = idx - 1
	region.Text = regions.members[idx-1].text
	region.Join = 0
	region.Comment = makeText(rCmntLen, rCmntSd)
	return region
}
