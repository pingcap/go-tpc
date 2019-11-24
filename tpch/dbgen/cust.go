package dbgen

type Cust struct {
}

func (c Cust) loader() error {
	panic("implement me")
}

func sdCust(child table, skipCount dssHuge) {
	advanceStream(C_ADDR_SD, skipCount*9, false)
	advanceStream(C_CMNT_SD, skipCount*2, false)
	advanceStream(C_NTRG_SD, skipCount, false)
	advanceStream(C_PHNE_SD, skipCount*3, false)
	advanceStream(C_ABAL_SD, skipCount, false)
	advanceStream(C_MSEG_SD, skipCount, false)
}
