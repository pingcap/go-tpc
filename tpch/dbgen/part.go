package dbgen

type Part struct{}

func (p Part) loader() error {
	panic("implement me")
}

func sdPart(child table, skipCount dssHuge) {
	for i := P_MFG_SD; i <= P_CNTR_SD; i++ {
		advanceStream(i, skipCount, false)
	}
	advanceStream(P_CMNT_SD, skipCount*2, false)
	advanceStream(P_NAME_SD, skipCount*92, false)
}

func partSuppBridge(idx, s dssHuge) dssHuge {
	panic("implement me")
}
