package dbgen

import (
	"fmt"
	"io"
)

const (
	cPhneSd  = 28
	cAbalSd  = 29
	cMsegSd  = 30
	cAddrLen = 25
	cCmntLen = 73
	cAddrSd  = 26
	cCmntSd  = 31
	cAbalMin = -99999
	cAbalMax = 999999
	lNtrgSd  = 27
)

type Cust struct {
	CustKey    dssHuge
	Name       string
	Address    string
	NationCode dssHuge
	Phone      string
	Acctbal    dssHuge
	MktSegment string
	Comment    string
}

type custLoader struct {
	io.StringWriter
}

func (c custLoader) Load(item interface{}) error {
	cust := item.(*Cust)
	if _, err := c.WriteString(
		fmt.Sprintf("%d|%s|%s|%d|%s|%s|%s|%s|\n",
			cust.CustKey,
			cust.Name,
			cust.Address,
			cust.NationCode,
			cust.Phone,
			FmtMoney(cust.Acctbal),
			cust.MktSegment,
			cust.Comment)); err != nil {
		return err
	}
	return nil
}

func (c custLoader) Flush() error {
	return nil
}

func newCustLoader(writer io.StringWriter) custLoader {
	return custLoader{writer}
}

func sdCust(child Table, skipCount dssHuge) {
	advanceStream(cAddrSd, skipCount*9, false)
	advanceStream(cCmntSd, skipCount*2, false)
	advanceStream(lNtrgSd, skipCount, false)
	advanceStream(cPhneSd, skipCount*3, false)
	advanceStream(cAbalSd, skipCount, false)
	advanceStream(cMsegSd, skipCount, false)
}

func makeCust(idx dssHuge) *Cust {
	cust := &Cust{}
	cust.CustKey = idx
	cust.Name = fmt.Sprintf("Customer#%09d", idx)
	cust.Address = vStr(cAddrLen, cAddrSd)
	i := random(0, dssHuge(nations.count-1), lNtrgSd)
	cust.NationCode = i
	cust.Phone = genPhone(i, cPhneSd)
	cust.Acctbal = random(cAbalMin, cAbalMax, cAbalSd)
	pickStr(&cMsegSet, cMsegSd, &cust.MktSegment)
	cust.Comment = makeText(cCmntLen, cCmntSd)

	return cust
}
