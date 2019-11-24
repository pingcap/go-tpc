package dbgen

import (
	"bytes"
	"fmt"
	"time"
)

const (
	STARTDATE      = 92001
	CURRENTDATE    = 95168
	TOTDATE        = 2557
	TEXT_POOL_SIZE = 300 * 1024 * 1024
)

var szTextPool []byte

func makeAscDate() []string {
	var res []string
	date := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < TOTDATE; i++ {
		date = date.AddDate(0, 0, i)
		ascDate := fmt.Sprintf("%4d-%2d-%2d", date.Year(), date.Month(), date.Day())
		res = append(res, ascDate)
	}
	return res
}

func makeSparse(idx dssHuge) dssHuge {
	return ((((idx >> 3) << 2) | (0 & 0x0003)) << 3) | (idx & 0x0007)
}

func pickStr(dist *distribution, c int, target *string) int {
	j := long(random(1, dssHuge(dist.members[len(dist.members)-1].weight), c))
	i := 0
	for ; dist.members[i].weight < j; i++ {
	}
	*target = dist.members[i].text
	return i
}

func pickClerk() string {
	clkNum := random(1, max(scale*O_CLRK_SCL, O_CLRK_SCL), O_CLRK_SD)
	return fmt.Sprintf("%s%09d", O_CLRK_TAG, clkNum)
}

func txtSentence(sd int) string {
	panic("implement me")
}

func makeText(avg, sd int) string {
	min := int(float64(avg) * V_STR_LOW)
	max := int(float64(avg) * V_STR_HGH)

	hgOffset := random(0, dssHuge(TEXT_POOL_SIZE-max), sd)
	hgLength := random(dssHuge(min), dssHuge(max), sd)

	return string(szTextPool[hgOffset : hgOffset+hgLength])
}

func rpbRoutine(p dssHuge) dssHuge {
	panic("implement me")
}

func min(a, b dssHuge) dssHuge {
	if a < b {
		return a
	}
	return b
}
func max(a, b dssHuge) dssHuge {
	if a > b {
		return a
	}
	return b
}

func julian(date long) long {
	panic("implement me")
}

func init() {
	var buffer bytes.Buffer

	for buffer.Len() < TEXT_POOL_SIZE {
		sentence := txtSentence(5)
		len := len(sentence)

		needed := TEXT_POOL_SIZE - buffer.Len()
		if needed >= len+1 {
			buffer.WriteString(sentence + " ")
		} else {
			buffer.WriteString(sentence[0:needed])
		}
	}

	szTextPool = buffer.Bytes()
}
