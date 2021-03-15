package dbgen

import (
	"fmt"
	"sync"
)

type Table int
type dssHuge int64
type long int64

var (
	scale dssHuge
)

type tDef struct {
	name    string
	comment string
	base    dssHuge
	loader  Loader
	genSeed func(Table, dssHuge)
	child   Table
	vTotal  dssHuge
}

var tDefs []tDef

func genTbl(tnum Table, start, count dssHuge) error {
	loader := tDefs[tnum].loader
	defer loader.Flush()

	for i := start; i < start+count; i++ {
		rowStart(tnum)
		switch tnum {
		case TLine:
			fallthrough
		case TOrder:
			fallthrough
		case TOrderLine:
			order := makeOrder(i)
			if err := loader.Load(order); err != nil {
				return err
			}
		case TSupp:
			supp := makeSupp(i)
			if err := loader.Load(supp); err != nil {
				return err
			}
		case TCust:
			cust := makeCust(i)
			if err := loader.Load(cust); err != nil {
				return err
			}
		case TPsupp:
			fallthrough
		case TPart:
			fallthrough
		case TPartPsupp:
			part := makePart(i)
			if err := loader.Load(part); err != nil {
				return err
			}
		case TNation:
			nation := makeNation(i)
			if err := loader.Load(nation); err != nil {
				return err
			}
		case TRegion:
			region := makeRegion(i)
			if err := loader.Load(region); err != nil {
				return err
			}
		}
		rowStop(tnum)
	}
	return nil
}

func initTDefs() {
	tDefs = []tDef{
		{"part.tbl", "part table", 200000, nil, sdPart, TPsupp, 0},
		{"partsupp.tbl", "partsupplier table", 200000, nil, sdPsupp, TNone, 0},
		{"supplier.tbl", "suppliers table", 10000, nil, sdSupp, TNone, 0},
		{"customer.tbl", "customers table", 150000, nil, sdCust, TNone, 0},
		{"orders.tbl", "order table", 150000 * ordersPerCust, nil, sdOrder, TLine, 0},
		{"lineitem.tbl", "lineitem table", 150000 * ordersPerCust, nil, sdLineItem, TNone, 0},
		{"orders.tbl", "orders/lineitem tables", 150000 * ordersPerCust, newOrderLineLoader(), sdOrder, TLine, 0},
		{"part.tbl", "part/partsupplier tables", 200000, newPartPsuppLoader(), sdPart, TPsupp, 0},
		{"nation.tbl", "nation table", dssHuge(nations.count), nil, sdNull, TNone, 0},
		{"region.tbl", "region table", dssHuge(regions.count), nil, sdNull, TNone, 0},
	}
}

func InitDbGen(sc int64) {
	scale = dssHuge(sc)
	initSeeds()
	initDists()
	initTextPool()

	initTDefs()
	initOrder()
	initLineItem()
}

func DbGen(loaders map[Table]Loader, tables []Table) error {
	for table, loader := range loaders {
		tDefs[table].loader = loader
	}

	wg := sync.WaitGroup{}
	wg.Add(len(tables))
	for _, i := range tables {
		go func(i Table) {
			fmt.Printf("generating %s\n", tDefs[i].comment)
			defer wg.Done()
			rowCnt := tDefs[i].base
			if i < TNation {
				rowCnt *= scale
			}
			if err := genTbl(i, 1, rowCnt); err != nil {
				fmt.Errorf("fail to generate %s, err: %v", tDefs[i].name, err)
				return
			}
			fmt.Printf("generate %s done\n", tDefs[i].comment)
		}(i)
	}
	wg.Wait()
	return nil
}
