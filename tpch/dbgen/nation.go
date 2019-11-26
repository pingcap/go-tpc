package dbgen

import (
	"fmt"
	"os"
)

type Nation struct {
	code    dssHuge
	text    string
	join    long
	comment string
}

func makeNation(idx dssHuge) *Nation {
	nation := &Nation{}
	nation.code = idx - 1
	nation.text = nations.members[idx-1].text
	nation.join = nations.members[idx-1].weight
	nation.comment = makeText(N_CMNT_LEN, N_CMNT_SD)

	return nation
}

var _nationLoader = func(nation interface{}) error {
	n := nation.(*Nation)
	f, err := os.OpenFile(tDefs[NATION].name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(
		fmt.Sprintf("%d|%s|%d|%s|\n",
			n.code,
			n.text,
			n.join,
			n.comment)); err != nil {
		return err
	}
	return nil
}

var nationLoader = &_nationLoader
