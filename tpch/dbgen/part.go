package dbgen

import (
	"fmt"
	"os"
)

type Part struct {
	partKey     dssHuge
	name        string
	mfgr        string
	brand       string
	types       string
	size        dssHuge
	container   string
	retailPrice dssHuge
	comment     string
	s           []PartSupp
}

func sdPart(child table, skipCount dssHuge) {
	for i := P_MFG_SD; i <= P_CNTR_SD; i++ {
		advanceStream(i, skipCount, false)
	}
	advanceStream(P_CMNT_SD, skipCount*2, false)
	advanceStream(P_NAME_SD, skipCount*92, false)
}

func partSuppBridge(p, s dssHuge) dssHuge {
	totScnt := tDefs[SUPP].base * scale
	return (p+s*(totScnt/SUPP_PER_PART+((p-1)/totScnt)))%totScnt + 1
}

var _partLoader = func(part interface{}) error {
	p := part.(*Part)
	f, err := os.OpenFile(tDefs[PART].name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(
		fmt.Sprintf("%d|%s|%s|%s|%s|%d|%s|%s|%s|\n",
			p.partKey,
			p.name,
			p.mfgr,
			p.brand,
			p.types,
			p.size,
			p.container,
			fmtMoney(p.retailPrice),
			p.comment)); err != nil {
		return err
	}
	return nil
}
var partLoader = &_partLoader

func makePart(idx dssHuge) *Part {
	part := &Part{}
	part.partKey = idx
	part.name = aggStr(&colors, P_NAME_SCL, P_NAME_SD)
	tmp := random(P_MFG_MIN, P_MFG_MAX, P_MFG_SD)
	part.mfgr = fmt.Sprintf("Manufacturer#%d", tmp)
	brnd := random(P_BRND_MIN, P_BRND_MAX, P_BRND_SD)
	part.brand = fmt.Sprintf("Brand#%02d", tmp*10+brnd)
	pickStr(&pTypesSet, P_TYPE_SD, &part.types)
	part.size = random(P_SIZE_MIN, P_SIZE_MAX, P_SIZE_SD)
	pickStr(&pCntrSet, P_CNTR_SD, &part.container)
	part.retailPrice = rpbRoutine(idx)
	part.comment = makeText(P_CMNT_LEN, P_CMNT_SD)

	for snum := 0; snum < SUPP_PER_PART; snum++ {
		ps := PartSupp{}
		ps.partKey = part.partKey
		ps.suppKey = partSuppBridge(idx, dssHuge(snum))
		ps.qty = random(PS_QTY_MIN, PS_QTY_MAX, PS_QTY_SD)
		ps.sCost = random(PS_SCST_MIN, PS_SCST_MAX, PS_SCST_SD)
		ps.comment = makeText(PS_CMNT_LEN, PS_CMNT_SD)
		part.s = append(part.s, ps)
	}

	return part
}
