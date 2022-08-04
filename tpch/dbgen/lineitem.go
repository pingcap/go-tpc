package dbgen

import (
	"context"
	"io"

	"github.com/pingcap/go-tpc/pkg/sink"
)

const (
	lQtyMin  = 1
	lQtyMax  = 50
	lTaxMin  = 0
	lTaxMax  = 8
	lDcntMin = 0
	lDcntMax = 10
	lPkeyMin = 1
	lSdteMin = 1
	lSdteMax = 121
	lCdteMin = 30
	lCdteMax = 90
	lRdteMin = 1
	lRdteMax = 30
)

var (
	LPkeyMax dssHuge
)

type LineItem struct {
	OKey         dssHuge
	PartKey      dssHuge
	SuppKey      dssHuge
	LCnt         dssHuge
	Quantity     dssHuge
	EPrice       dssHuge
	Discount     dssHuge
	Tax          dssHuge
	RFlag        string
	LStatus      string
	CDate        string
	SDate        string
	RDate        string
	ShipInstruct string
	ShipMode     string
	Comment      string
}

type lineItemLoader struct {
	*sink.CSVSink
}

func (l lineItemLoader) Load(item interface{}) error {
	o := item.(*Order)
	for _, line := range o.Lines {
		if err := l.WriteRow(context.TODO(),
			line.OKey,
			line.PartKey,
			line.SuppKey,
			line.LCnt,
			line.Quantity,
			FmtMoney(line.EPrice),
			FmtMoney(line.Discount),
			FmtMoney(line.Tax),
			line.RFlag,
			line.LStatus,
			line.SDate,
			line.CDate,
			line.RDate,
			line.ShipInstruct,
			line.ShipMode,
			line.Comment,
		); err != nil {
			return err
		}
	}
	return nil
}

func (l lineItemLoader) Flush() error {
	return l.CSVSink.Flush(context.TODO())
}

func NewLineItemLoader(w io.Writer) lineItemLoader {
	return lineItemLoader{sink.NewCSVSinkWithDelimiter(w, '|')}
}

func sdLineItem(child Table, skipCount dssHuge) {
	for j := 0; j < oLcntMax; j++ {
		for i := lQtySd; i <= lRflgSd; i++ {
			advanceStream(i, skipCount, false)
		}
		advanceStream(lCmntSd, skipCount*2, false)
	}
	if child == TPsupp {
		advanceStream(oOdateSd, skipCount, false)
		advanceStream(oLcntSd, skipCount, false)
	}
}

func initLineItem() {
	LPkeyMax = tDefs[TPart].base * scale
}
