package dbgen

const (
	L_SIZE     = 144
	L_QTY_MIN  = 1
	L_QTY_MAX  = 50
	L_TAX_MIN  = 0
	L_TAX_MAX  = 8
	L_DCNT_MIN = 0
	L_DCNT_MAX = 10
	L_PKEY_MIN = 1
	L_SDTE_MIN = 1
	L_SDTE_MAX = 121
	L_CDTE_MIN = 30
	L_CDTE_MAX = 90
	L_RDTE_MIN = 1
	L_RDTE_MAX = 30
)

var (
	L_PKEY_MAX dssHuge
)

type LineItem struct {
	oKey         dssHuge
	partKey      dssHuge
	suppKey      dssHuge
	lCnt         dssHuge
	quantity     dssHuge
	ePrice       dssHuge
	discount     dssHuge
	tax          dssHuge
	rFlag        byte
	lStatus      byte
	cDate        string
	sDate        string
	rDate        string
	shipInstruct string
	shipMode     string
	comment      string
}

func (l LineItem) loader() error {
	panic("implement me")
}

func sdLineItem(child table, skipCount dssHuge) {
	for j := 0; j < O_LCNT_MAX; j++ {
		for i := L_QTY_SD; i <= L_RFLG_SD; i++ {
			advanceStream(i, skipCount, false)
		}
		advanceStream(L_CMNT_SD, skipCount*2, false)
	}
	if child == PSUPP {
		advanceStream(O_ODATE_SD, skipCount, false)
		advanceStream(O_LCNT_SD, skipCount, false)
	}
}

func init() {
	L_PKEY_MAX = tDefs[PART].base * scale
}