package dbgen

type table int
type dssHuge int64
type long int32

const (
	N_CMNT_LEN  = 72
	N_CMNT_MAX  = 152
	R_CMNT_LEN  = 72
	R_CMNT_MAX  = 152
	MONEY_SCL   = 0.01
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
	P_MFG_SD = iota
	P_BRND_SD
	P_TYPE_SD
	P_SIZE_SD
	P_CNTR_SD
	P_RCST_SD
	PS_QTY_SD
	PS_SCST_SD
	O_SUPP_SD
	O_CLRK_SD
	O_ODATE_SD
	L_QTY_SD
	L_DCNT_SD
	L_TAX_SD
	L_SHIP_SD
	L_SMODE_SD
	L_PKEY_SD
	L_SKEY_SD
	L_SDTE_SD
	L_CDTE_SD
	L_RDTE_SD
	L_RFLG_SD
	C_NTRG_SD
	C_PHNE_SD
	C_ABAL_SD
	C_MSEG_SD
	S_NTRG_SD
	S_PHNE_SD
	S_ABAL_SD
	P_NAME_SD
	O_PRIO_SD
	HVAR_SD
	O_CKEY_SD
	N_CMNT_SD
	R_CMNT_SD
	O_LCNT_SD
	BBB_JNK_SD
	BBB_TYPE_SD
	BBB_CMNT_SD
	BBB_OFFSET_SD7
)

const (
	PENNIES = 100
)

var (
	scale dssHuge
)

type tDef struct {
	name    string
	comment string
	base    dssHuge
	genSeed func(table, dssHuge) long
	child   table
	vTotal  dssHuge
}

var tDefs []tDef

// GenData generate data
func genTable(n table, start, count dssHuge) error {
	for i := start; i < start+count; i++ {
		switch n {
		case LINE:
			fallthrough
		case ORDER:
			fallthrough
		case ORDER_LINE:
			order := makeOrder(i)
			if err := order.loader(); err != nil {
				return err
			}
			//case SUPP:
			//	d.makeSupp(i)
			//case CUST:
			//	d.makeCust(i)
			//case PSUPP:
			//	fallthrough
			//case PART:
			//	fallthrough
			//case PARTPSUPP:
			//	d.makePart(i)
			//case NATION:
			//	d.makeNation(i)
			//case REGION:
			//	d.makeRegion(i)
		}
	}
}

func init() {
	tDefs = []tDef{
		{"part.tbl", "part table", 200000, sdPart, PSUPP, 0},
		{"partsupp.tbl", "partsupplier table", 200000, sdPsupp, NONE, 0},
		{"supplier.tbl", "suppliers table", 10000, sdSupp, NONE, 0},
		{"customer.tbl", "customers table", 150000, sdCust, NONE, 0},
		{"orders.tbl", "order table", 150000, sdOrder, LINE, 0},
		{"lineitem.tbl", "lineitem table", 150000, sdLineItem, NONE, 0},
		{"orders.tbl", "orders/lineitem tables", 150000, sdOrder, LINE, 0},
		{"part.tbl", "part/partsupplier tables", 200000, sdPart, PSUPP, 0},
		{"nation.tbl", "nation table", 90, sdNull, NONE, 0},
		{"region.tbl", "region table", 90, sdNull, NONE, 0},
	}
}
