package dbgen

type Psupp struct{}

func sdPsupp(child table, skipCount dssHuge) {
	for j := 0; j < SUPP_PER_PART; j++ {
		advanceStream(PS_QTY_SD, skipCount, false)
		advanceStream(PS_SCST_SD, skipCount, false)
		advanceStream(PS_CMNT_SD, skipCount*2, false)
	}
}
