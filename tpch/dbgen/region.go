package dbgen

import (
	"fmt"
	"os"
)

type Region struct {
	code    dssHuge
	text    string
	join    long
	comment string
}

var _regionLoader = func(region interface{}) error {
	r := region.(*Region)
	f, err := os.OpenFile(tDefs[REGION].name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(
		fmt.Sprintf("%d|%s|%s|\n",
			r.code,
			r.text,
			r.comment)); err != nil {
		return err
	}
	return nil
}

var regionLoader = &_regionLoader

func makeRegion(idx dssHuge) *Region {
	region := &Region{}

	region.code = idx - 1
	region.text = regions.members[idx-1].text
	region.join = 0
	region.comment = makeText(R_CMNT_LEN, R_CMNT_SD)
	return region
}
