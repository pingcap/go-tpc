package dbgen

import (
	"fmt"
	"io"
)

const (
	sNtrgSd      = 33
	sPhneSd      = 34
	sAbalSd      = 35
	sAddrLen     = 25
	sAbalMin     = -99999
	sAbalMax     = 999999
	sCmntLen     = 63
	sAddrSd      = 32
	sCmntSd      = 36
	sCmntBbb     = 10
	bbbJnkSd     = 44
	bbbTypeSd    = 45
	bbbCmntSd    = 46
	bbbOffsetSd  = 47
	bbbDeadbeats = 50
	bbbBase      = "Customer "
	bbbComplain  = "Complaints"
	bbbCommend   = "Recommends"
	bbbCmntLen   = 19
	bbbBaseLen   = 9
)

type Supp struct {
	SuppKey    dssHuge
	Name       string
	Address    string
	NationCode dssHuge
	Phone      string
	Acctbal    dssHuge
	Comment    string
}

type suppLoader struct {
	io.StringWriter
}

func (s suppLoader) Load(item interface{}) error {
	supp := item.(*Supp)
	if _, err := s.WriteString(
		fmt.Sprintf("%d|%s|%s|%d|%s|%s|%s|\n",
			supp.SuppKey,
			supp.Name,
			supp.Address,
			supp.NationCode,
			supp.Phone,
			FmtMoney(supp.Acctbal),
			supp.Comment)); err != nil {
		return err
	}
	return nil
}

func (s suppLoader) Flush() error {
	return nil
}

func NewSuppLoader(writer io.StringWriter) suppLoader {
	return suppLoader{writer}
}

func makeSupp(idx dssHuge) *Supp {
	supp := &Supp{}
	supp.SuppKey = idx
	supp.Name = fmt.Sprintf("Supplier#%09d", idx)
	supp.Address = vStr(sAddrLen, sAddrSd)
	i := random(0, dssHuge(nations.count-1), sNtrgSd)
	supp.NationCode = i
	supp.Phone = genPhone(i, sPhneSd)
	supp.Acctbal = random(sAbalMin, sAbalMax, sAbalSd)
	supp.Comment = makeText(sCmntLen, sCmntSd)

	badPress := random(1, 10000, bbbCmntSd)
	types := random(0, 100, bbbTypeSd)
	noise := random(0, dssHuge(len(supp.Comment)-bbbCmntLen), bbbJnkSd)
	offset := random(0, dssHuge(len(supp.Comment))-(bbbCmntLen+noise), bbbOffsetSd)

	if badPress <= sCmntBbb {
		if types < bbbDeadbeats {
			types = 0
		} else {
			types = 1
		}
		supp.Comment = supp.Comment[:offset] + bbbBase + supp.Comment[offset+dssHuge(len(bbbBase)):]
		if types == 0 {
			supp.Comment = supp.Comment[:bbbBaseLen+offset+noise] +
				bbbComplain +
				supp.Comment[bbbBaseLen+offset+noise+dssHuge(len(bbbComplain)):]
		} else {
			supp.Comment = supp.Comment[:bbbBaseLen+offset+noise] +
				bbbCommend +
				supp.Comment[bbbBaseLen+offset+noise+dssHuge(len(bbbCommend)):]
		}
	}

	return supp
}

func sdSupp(child Table, skipCount dssHuge) {
	advanceStream(sNtrgSd, skipCount, false)
	advanceStream(sPhneSd, skipCount*3, false)
	advanceStream(sAbalSd, skipCount, false)
	advanceStream(sAddrSd, skipCount*9, false)
	advanceStream(sCmntSd, skipCount*2, false)
	advanceStream(bbbCmntSd, skipCount, false)
	advanceStream(bbbJnkSd, skipCount, false)
	advanceStream(bbbOffsetSd, skipCount, false)
	advanceStream(bbbTypeSd, skipCount, false)
}
