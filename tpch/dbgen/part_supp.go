package dbgen

import (
	"context"
	"io"

	"github.com/pingcap/go-tpc/pkg/sink"
)

type PartSupp struct {
	PartKey dssHuge
	SuppKey dssHuge
	Qty     dssHuge
	SCost   dssHuge
	Comment string
}

func sdPsupp(child Table, skipCount dssHuge) {
	for j := 0; j < suppPerPart; j++ {
		advanceStream(psQtySd, skipCount, false)
		advanceStream(psScstSd, skipCount, false)
		advanceStream(psCmntSd, skipCount*2, false)
	}
}

type partSuppLoader struct {
	*sink.CSVSink
}

func (p partSuppLoader) Load(item interface{}) error {
	pSupp := item.(*Part)
	for i := 0; i < suppPerPart; i++ {
		supp := pSupp.S[i]
		if err := p.WriteRow(context.TODO(),
			supp.PartKey,
			supp.SuppKey,
			supp.Qty,
			FmtMoney(supp.SCost),
			supp.Comment); err != nil {
			return err
		}
	}
	return nil
}

func (p partSuppLoader) Flush() error {
	return p.CSVSink.Flush(context.TODO())
}

func NewPartSuppLoader(w io.Writer) partSuppLoader {
	return partSuppLoader{sink.NewCSVSinkWithDelimiter(w, '|')}
}
