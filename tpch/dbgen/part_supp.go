package dbgen

import (
	"fmt"
	"os"
)

type PartSupp struct {
	partKey dssHuge
	suppKey dssHuge
	qty     dssHuge
	sCost   dssHuge
	comment string
}

func sdPsupp(child table, skipCount dssHuge) {
	for j := 0; j < SUPP_PER_PART; j++ {
		advanceStream(PS_QTY_SD, skipCount, false)
		advanceStream(PS_SCST_SD, skipCount, false)
		advanceStream(PS_CMNT_SD, skipCount*2, false)
	}
}

var _partSuppLoader = func(part interface{}) error {
	p := part.(*Part)
	f, err := os.OpenFile(tDefs[PSUPP].name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	for i := 0; i < SUPP_PER_PART; i++ {
		supp := p.s[i]
		if _, err := f.WriteString(
			fmt.Sprintf("%d|%d|%d|%s|%s|\n",
				supp.partKey,
				supp.suppKey,
				supp.qty,
				fmtMoney(supp.sCost),
				supp.comment)); err != nil {
			return err
		}
	}

	return nil
}

var partSuppLoader = &_partSuppLoader
