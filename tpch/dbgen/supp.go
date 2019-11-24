package dbgen

type Supp struct{}

func (s Supp) loader() error {
	panic("implement me")
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
