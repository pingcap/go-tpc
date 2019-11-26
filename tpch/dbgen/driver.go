package dbgen

type table int
type dssHuge int64
type long int64

const (
	N_CMNT_LEN  = 72
	N_CMNT_MAX  = 152
	R_CMNT_LEN  = 72
	R_CMNT_MAX  = 152
	MONEY_SCL   = 0.01
	V_STR_LOW   = 0.4
	V_STR_HGH   = 1.6
	P_NAME_LEN  = 55
	P_MFG_LEN   = 25
	P_BRND_LEN  = 10
	P_TYPE_LEN  = 25
	P_CNTR_LEN  = 10
	P_CMNT_LEN  = 14
	P_CMNT_MAX  = 23
	S_NAME_LEN  = 25
	S_ADDR_LEN  = 25
	S_ADDR_MAX  = 40
	S_CMNT_LEN  = 63
	S_CMNT_MAX  = 101
	PS_CMNT_LEN = 124
	PS_CMNT_MAX = 199
	C_NAME_LEN  = 18
	C_ADDR_LEN  = 25
	C_ADDR_MAX  = 40
	C_MSEG_LEN  = 10
	C_CMNT_LEN  = 73
	C_CMNT_MAX  = 117
	O_OPRIO_LEN = 15
	O_CLRK_LEN  = 15
	O_CMNT_LEN  = 49
	O_CMNT_MAX  = 79
	L_CMNT_LEN  = 27
	L_CMNT_MAX  = 44
	L_INST_LEN  = 25
	L_SMODE_LEN = 10
	T_ALPHA_LEN = 10
	DATE_LEN    = 13
	NATION_LEN  = 25
	REGION_LEN  = 25
	PHONE_LEN   = 15
	MAXAGG_LEN  = 20
	P_CMNT_SD   = 6
	PS_CMNT_SD  = 9
	O_CMNT_SD   = 12
	C_ADDR_SD   = 26
	C_CMNT_SD   = 31
	S_ADDR_SD   = 32
	S_CMNT_SD   = 36
	L_CMNT_SD   = 25
)

const (
	P_MFG_SD      = 0
	P_BRND_SD     = 1
	P_TYPE_SD     = 2
	P_SIZE_SD     = 3
	P_CNTR_SD     = 4
	P_RCST_SD     = 5
	PS_QTY_SD     = 7
	PS_SCST_SD    = 8
	O_SUPP_SD     = 10
	O_CLRK_SD     = 11
	O_ODATE_SD    = 13
	L_QTY_SD      = 14
	L_DCNT_SD     = 15
	L_TAX_SD      = 16
	L_SHIP_SD     = 17
	L_SMODE_SD    = 18
	L_PKEY_SD     = 19
	L_SKEY_SD     = 20
	L_SDTE_SD     = 21
	L_CDTE_SD     = 22
	L_RDTE_SD     = 23
	L_RFLG_SD     = 24
	C_NTRG_SD     = 27
	C_PHNE_SD     = 28
	C_ABAL_SD     = 29
	C_MSEG_SD     = 30
	S_NTRG_SD     = 33
	S_PHNE_SD     = 34
	S_ABAL_SD     = 35
	P_NAME_SD     = 37
	O_PRIO_SD     = 38
	HVAR_SD       = 39
	O_CKEY_SD     = 40
	N_CMNT_SD     = 41
	R_CMNT_SD     = 42
	O_LCNT_SD     = 43
	BBB_JNK_SD    = 44
	BBB_TYPE_SD   = 45
	BBB_CMNT_SD   = 46
	BBB_OFFSET_SD = 47
)

const (
	PENNIES       = 100
	SUPP_PER_PART = 4
	S_SIZE        = 145
	S_ABAL_MIN    = -99999
	S_ABAL_MAX    = 999999
	S_CMNT_BBB    = 10
	BBB_DEADBEATS = 50
	BBB_BASE      = "Customer "
	BBB_COMPLAIN  = "Complaints"
	BBB_COMMEND   = "Recommends"
	BBB_CMNT_LEN  = 19
	BBB_BASE_LEN  = 9
	BBB_TYPE_LEN  = 10
	O_CLRK_TAG    = "Clerk#"
	O_CLRK_FMT    = "%%s%%0%d%s"
	O_CLRK_SCL    = 1000
	NATIONS_MAX   = 90

	C_ABAL_MIN = -99999
	C_ABAL_MAX = 999999

	P_NAME_SCL  = 5
	P_MFG_MIN   = 1
	P_MFG_MAX   = 5
	P_BRND_MIN  = 1
	P_BRND_MAX  = 5
	P_SIZE_MIN  = 1
	P_SIZE_MAX  = 50
	PS_QTY_MIN  = 1
	PS_QTY_MAX  = 9999
	PS_SCST_MIN = 100
	PS_SCST_MAX = 100000
)

var (
	scale dssHuge
)

type tDef struct {
	name    string
	comment string
	base    dssHuge
	loader  *func(interface{}) error
	genSeed func(table, dssHuge)
	child   table
	vTotal  dssHuge
}

var tDefs []tDef

// GenData generate data
func genTable(tnum table, start, count dssHuge) error {
	loader := *tDefs[tnum].loader
	for i := start; i < start+count; i++ {
		rowStart(tnum)
		switch tnum {
		case LINE:
			fallthrough
		case ORDER:
			fallthrough
		case ORDER_LINE:
			order := makeOrder(i)
			if err := loader(order); err != nil {
				return err
			}
		case SUPP:
			supp := makeSupp(i)
			if err := loader(supp); err != nil {
				return err
			}
		case CUST:
			cust := makeCust(i)
			if err := loader(cust); err != nil {
				return err
			}
		case PSUPP:
			fallthrough
		case PART:
			fallthrough
		case PART_PSUPP:
			part := makePart(i)
			if err := loader(part); err != nil {
				return err
			}
			//case NATION:
			//	d.makeNation(i)
			//case REGION:
			//	d.makeRegion(i)
		}
		rowStop(tnum)
	}
	return nil
}

func sdNull(child table, skipCount dssHuge) {
}

var notImplLoader = func(order interface{}) error {
	panic("implement me")
}

func initTDefs() {
	tDefs = []tDef{
		{"part.tbl", "part table", 200000, partLoader, sdPart, PSUPP, 0},
		{"partsupp.tbl", "partsupplier table", 200000, partSuppLoader, sdPsupp, NONE, 0},
		{"supplier.tbl", "suppliers table", 10000, suppLoader, sdSupp, NONE, 0},
		{"customer.tbl", "customers table", 150000, custLoader, sdCust, NONE, 0},
		{"orders.tbl", "order table", 150000, orderLoader, sdOrder, LINE, 0},
		{"lineitem.tbl", "lineitem table", 150000, lineItemLoader, sdLineItem, NONE, 0},
		{"orders.tbl", "orders/lineitem tables", 150000, orderLineLoader, sdOrder, LINE, 0},
		{"part.tbl", "part/partsupplier tables", 200000, partPsuppLoader, sdPart, PSUPP, 0},
		{"nation.tbl", "nation table", dssHuge(nations.count), &notImplLoader, sdNull, NONE, 0},
		{"region.tbl", "region table", dssHuge(regions.count), &notImplLoader, sdNull, NONE, 0},
	}
}

func initDriver(sc int) {
	scale = dssHuge(sc)
	initSeeds()
	initDists()
	initTextPool()

	initTDefs()
	initOrder()
	initLineItem()
}
