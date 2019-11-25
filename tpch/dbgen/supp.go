package dbgen

import (
	"fmt"
	"os"
)

type Supp struct {
	suppKey    dssHuge
	name       string
	address    string
	nationCode dssHuge
	phone      string
	acctbal    dssHuge
	comment    string
}

var _suppLoader = func(supp interface{}) error {
	s := supp.(*Supp)
	f, err := os.OpenFile(tDefs[SUPP].name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(
		fmt.Sprintf("%d|%s|%s|%d|%s|%s|%s|\n",
			s.suppKey,
			s.name,
			s.address,
			s.nationCode,
			s.phone,
			fmtMoney(s.acctbal),
			s.comment)); err != nil {
		return err
	}
	return nil
}
var suppLoader = &_suppLoader

func makeSupp(idx dssHuge) *Supp {
	supp := &Supp{}
	supp.suppKey = idx
	supp.name = fmt.Sprintf("Supplier#%09d", idx)
	supp.address = vStr(S_ADDR_LEN, S_ADDR_SD)
	i := random(0, dssHuge(nations.count-1), S_NTRG_SD)
	supp.nationCode = i
	supp.phone = genPhone(i, S_PHNE_SD)
	supp.acctbal = random(S_ABAL_MIN, S_ABAL_MAX, S_ABAL_SD)
	supp.comment = makeText(S_CMNT_LEN, S_CMNT_SD)

	badPress := random(1, 10000, BBB_CMNT_SD)
	types := random(0, 100, BBB_TYPE_SD)
	noise := random(0, dssHuge(len(supp.comment)-BBB_CMNT_LEN), BBB_JNK_SD)
	offset := random(0, dssHuge(len(supp.comment))-(BBB_CMNT_LEN+noise), BBB_OFFSET_SD)

	if badPress <= S_CMNT_BBB {
		if types < BBB_DEADBEATS {
			types = 0
		} else {
			types = 1
		}
		supp.comment = supp.comment[:offset] + BBB_BASE + supp.comment[offset+dssHuge(len(BBB_BASE)):]
		if types == 0 {
			supp.comment = supp.comment[:BBB_BASE_LEN+offset+noise] +
				BBB_COMPLAIN +
				supp.comment[BBB_BASE_LEN+offset+noise+dssHuge(len(BBB_COMPLAIN)):]
		} else {
			supp.comment = supp.comment[:BBB_BASE_LEN+offset+noise] +
				BBB_COMMEND +
				supp.comment[BBB_BASE_LEN+offset+noise+dssHuge(len(BBB_COMMEND)):]
		}
	}

	return supp
}

func sdSupp(child table, skipCount dssHuge) {
	advanceStream(S_NTRG_SD, skipCount, false)
	advanceStream(S_PHNE_SD, skipCount*3, false)
	advanceStream(S_ABAL_SD, skipCount, false)
	advanceStream(S_ADDR_SD, skipCount*9, false)
	advanceStream(S_CMNT_SD, skipCount*2, false)
	advanceStream(BBB_CMNT_SD, skipCount, false)
	advanceStream(BBB_JNK_SD, skipCount, false)
	advanceStream(BBB_OFFSET_SD, skipCount, false)
	advanceStream(BBB_TYPE_SD, skipCount, false)
}
