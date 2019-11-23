package dbgen

const (
	STARTDATE   = 92001
	CURRENTDATE = 95168
	TOTDATE     = 2557
)

func makeAscDate() []string {
	panic("implement me")
}

func makeSparse(idx dssHuge) dssHuge {
	return ((((idx >> 3) << 2) | (0 & 0x0003)) << 3) | (idx & 0x0007)
}

func random64(lower, upper dssHuge, nStream long) dssHuge {
	panic("implement me")
}
func random(lower, upper dssHuge, nStream long) dssHuge {
	panic("implement me")
}

func pickStr(dist *distribution, c int) string {
	panic("implement me")
}

func pickClerk() string {
	panic("implement me")
}

func makeText(avg, sd int) string {
	panic("implement me")
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
