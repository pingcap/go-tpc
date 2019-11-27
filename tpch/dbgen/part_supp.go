package dbgen

import (
	"fmt"
	"io"
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
	io.StringWriter
}

func (p partSuppLoader) Load(item interface{}) error {
	pSupp := item.(*Part)
	for i := 0; i < suppPerPart; i++ {
		supp := pSupp.S[i]
		if _, err := p.WriteString(
			fmt.Sprintf("%d|%d|%d|%s|%s|\n",
				supp.PartKey,
				supp.SuppKey,
				supp.Qty,
				FmtMoney(supp.SCost),
				supp.Comment)); err != nil {
			return err
		}
	}
	return nil
}

func (p partSuppLoader) Flush() error {
	return nil
}

func newPartSuppLoader(writer io.StringWriter) partSuppLoader {
	return partSuppLoader{writer}
}
