package dbgen

import (
	"fmt"
	"io"
)

const (
	pNameScl    = 5
	pMfgMin     = 1
	pMfgMax     = 5
	pBrndMin    = 1
	pBrndMax    = 5
	pSizeMin    = 1
	pSizeMax    = 50
	psQtyMin    = 1
	psQtyMax    = 9999
	psScstMin   = 100
	psScstMax   = 100000
	pMfgSd      = 0
	pBrndSd     = 1
	pTypeSd     = 2
	pSizeSd     = 3
	pCntrSd     = 4
	psQtySd     = 7
	psScstSd    = 8
	pNameSd     = 37
	pCmntLen    = 14
	psCmntLen   = 124
	pCmntSd     = 6
	psCmntSd    = 9
	suppPerPart = 4
)

type Part struct {
	PartKey     dssHuge
	Name        string
	Mfgr        string
	Brand       string
	Type        string
	Size        dssHuge
	Container   string
	RetailPrice dssHuge
	Comment     string
	S           []PartSupp
}

func sdPart(child Table, skipCount dssHuge) {
	for i := pMfgSd; i <= pCntrSd; i++ {
		advanceStream(i, skipCount, false)
	}
	advanceStream(pCmntSd, skipCount*2, false)
	advanceStream(pNameSd, skipCount*92, false)
}

func partSuppBridge(p, s dssHuge) dssHuge {
	totScnt := tDefs[TSupp].base * scale
	return (p+s*(totScnt/suppPerPart+((p-1)/totScnt)))%totScnt + 1
}

type partLoader struct {
	io.StringWriter
}

func (p partLoader) Load(item interface{}) error {
	part := item.(*Part)
	if _, err := p.WriteString(
		fmt.Sprintf("%d|%s|%s|%s|%s|%d|%s|%s|%s|\n",
			part.PartKey,
			part.Name,
			part.Mfgr,
			part.Brand,
			part.Type,
			part.Size,
			part.Container,
			FmtMoney(part.RetailPrice),
			part.Comment)); err != nil {
		return err
	}
	return nil
}

func (p partLoader) Flush() error {
	return nil
}

func newPartLoader(writer io.StringWriter) partLoader {
	return partLoader{writer}
}

func makePart(idx dssHuge) *Part {
	part := &Part{}
	part.PartKey = idx
	part.Name = aggStr(&colors, pNameScl, pNameSd)
	tmp := random(pMfgMin, pMfgMax, pMfgSd)
	part.Mfgr = fmt.Sprintf("Manufacturer#%d", tmp)
	brnd := random(pBrndMin, pBrndMax, pBrndSd)
	part.Brand = fmt.Sprintf("Brand#%02d", tmp*10+brnd)
	pickStr(&pTypesSet, pTypeSd, &part.Type)
	part.Size = random(pSizeMin, pSizeMax, pSizeSd)
	pickStr(&pCntrSet, pCntrSd, &part.Container)
	part.RetailPrice = rpbRoutine(idx)
	part.Comment = makeText(pCmntLen, pCmntSd)

	for snum := 0; snum < suppPerPart; snum++ {
		ps := PartSupp{}
		ps.PartKey = part.PartKey
		ps.SuppKey = partSuppBridge(idx, dssHuge(snum))
		ps.Qty = random(psQtyMin, psQtyMax, psQtySd)
		ps.SCost = random(psScstMin, psScstMax, psScstSd)
		ps.Comment = makeText(psCmntLen, psCmntSd)
		part.S = append(part.S, ps)
	}

	return part
}
