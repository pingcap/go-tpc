package dbgen

import (
	"fmt"
	"os"
)

type Cust struct {
	custKey    dssHuge
	name       string
	address    string
	nationCode dssHuge
	phone      string
	acctbal    dssHuge
	mktSegment string
	comment    string
}

var _custLoader = func(cust interface{}) error {
	c := cust.(*Cust)
	f, err := os.OpenFile(tDefs[CUST].name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(
		fmt.Sprintf("%d|%s|%s|%d|%s|%s|%s|%s|\n",
			c.custKey,
			c.name,
			c.address,
			c.nationCode,
			c.phone,
			fmtMoney(c.acctbal),
			c.mktSegment,
			c.comment)); err != nil {
		return err
	}
	return nil
}
var custLoader = &_custLoader

func sdCust(child table, skipCount dssHuge) {
	advanceStream(C_ADDR_SD, skipCount*9, false)
	advanceStream(C_CMNT_SD, skipCount*2, false)
	advanceStream(C_NTRG_SD, skipCount, false)
	advanceStream(C_PHNE_SD, skipCount*3, false)
	advanceStream(C_ABAL_SD, skipCount, false)
	advanceStream(C_MSEG_SD, skipCount, false)
}

func makeCust(idx dssHuge) *Cust {
	cust := &Cust{}
	cust.custKey = idx
	cust.name = fmt.Sprintf("Customer#%09d", idx)
	cust.address = vStr(C_ADDR_LEN, C_ADDR_SD)
	i := random(0, dssHuge(nations.count-1), C_NTRG_SD)
	cust.nationCode = i
	cust.phone = genPhone(i, C_PHNE_SD)
	cust.acctbal = random(C_ABAL_MIN, C_ABAL_MAX, C_ABAL_SD)
	pickStr(&cMsegSet, C_MSEG_SD, &cust.mktSegment)
	cust.comment = makeText(C_CMNT_LEN, C_CMNT_SD)

	return cust
}
