package dbgen

import (
	"fmt"
	"io"
)

const (
	nCmntSd    = 41
	nCmntLen   = 72
	nationsMax = 90
)

type Nation struct {
	Code    dssHuge
	Text    string
	Join    long
	Comment string
}

func makeNation(idx dssHuge) *Nation {
	nation := &Nation{}
	nation.Code = idx - 1
	nation.Text = nations.members[idx-1].text
	nation.Join = nations.members[idx-1].weight
	nation.Comment = makeText(nCmntLen, nCmntSd)

	return nation
}

type nationLoader struct {
	io.StringWriter
}

func (n nationLoader) Load(item interface{}) error {
	nation := item.(*Nation)
	if _, err := n.WriteString(
		fmt.Sprintf("%d|%s|%d|%s|\n",
			nation.Code,
			nation.Text,
			nation.Join,
			nation.Comment)); err != nil {
		return err
	}
	return nil
}

func (n nationLoader) Flush() error {
	return nil
}

func newNationLoader(writer io.StringWriter) nationLoader {
	return nationLoader{writer}
}
