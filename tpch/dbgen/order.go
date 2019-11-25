package dbgen

import (
	"fmt"
	"os"
)

const (
	O_LCNT_MIN = 1
	O_LCNT_MAX = 7
)

var (
	ockeyMin dssHuge
	ockeyMax dssHuge
	odateMin dssHuge
	odateMax dssHuge
	ascDate  []string
)

type Order struct {
	oKey          dssHuge
	custKey       dssHuge
	status        byte
	totalPrice    dssHuge
	date          string
	orderPriority string
	clerk         string
	shipPriority  int64
	comment       string
	lines         []LineItem
}

var orderLoader = func(o *Order) error {
	f, err := os.OpenFile(tDefs[ORDER].name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(
		fmt.Sprintf("%d|%d|%c|%d.%02d|%s|%s|%s|%d|%s|\n",
			o.oKey,
			o.custKey,
			o.status,
			o.totalPrice/100, o.totalPrice%100,
			o.date,
			o.orderPriority,
			o.clerk,
			o.shipPriority,
			o.comment)); err != nil {
		return err
	}
	return nil
}

func (o Order) loader() error {
	return orderLoader(&o)
}

func sdOrder(child table, skipCount dssHuge) {
	advanceStream(O_LCNT_SD, skipCount, false)
	advanceStream(O_CKEY_SD, skipCount, false)
	advanceStream(O_CMNT_SD, skipCount*2, false)
	advanceStream(O_SUPP_SD, skipCount, false)
	advanceStream(O_CLRK_SD, skipCount, false)
	advanceStream(O_PRIO_SD, skipCount, false)
	advanceStream(O_ODATE_SD, skipCount, false)
}

func makeOrder(idx dssHuge) *Order {
	delta := 1
	order := &Order{}
	order.oKey = makeSparse(idx)
	if scale >= 30000 {
		order.custKey = random64(ockeyMin, ockeyMax, O_CKEY_SD)
	} else {
		order.custKey = random(ockeyMin, ockeyMax, O_CKEY_SD)
	}

	// Comment: Orders are not present for all customers.
	// In fact, one-third of the customers do not have any order in the database.
	// The orders are assigned at random to two-thirds of the customers
	for order.custKey%3 == 0 {
		order.custKey += dssHuge(delta)
		order.custKey = min(order.custKey, ockeyMax)
		delta *= -1
	}
	tmpDate := random(odateMin, odateMax, O_ODATE_SD)
	order.date = ascDate[tmpDate-STARTDATE]
	pickStr(&oPrioritySet, O_PRIO_SD, &order.orderPriority)
	order.clerk = pickClerk()
	order.comment = makeText(O_CMNT_LEN, O_CMNT_SD)
	order.shipPriority = 0
	order.totalPrice = 0
	order.status = 'O'
	oCnt := 0
	lineCount := random(O_LCNT_MIN, O_LCNT_MAX, O_LCNT_SD)

	for lCnt := dssHuge(0); lCnt < lineCount; lCnt++ {
		line := LineItem{}
		line.oKey = order.oKey
		line.lCnt = lCnt
		line.quantity = random(L_QTY_MIN, L_QTY_MAX, L_QTY_SD)
		line.discount = random(L_DCNT_MIN, L_DCNT_MAX, L_DCNT_SD)
		line.tax = random(L_TAX_MIN, L_TAX_MAX, L_TAX_SD)

		pickStr(&lInstructSet, L_SHIP_SD, &line.shipInstruct)
		pickStr(&lSmodeSet, L_SMODE_SD, &line.shipMode)
		line.comment = makeText(L_CMNT_LEN, L_CMNT_SD)

		if scale > 30000 {
			line.partKey = random64(L_PKEY_MIN, L_PKEY_MAX, L_PKEY_SD)
		} else {
			line.partKey = random(L_PKEY_MIN, L_PKEY_MAX, L_PKEY_SD)
		}

		rPrice := rpbRoutine(line.partKey)
		suppNum := random(0, 3, L_SKEY_SD)
		line.suppKey = partSuppBridge(line.partKey, suppNum)
		line.ePrice = rPrice * line.quantity

		order.totalPrice += ((line.ePrice * (100 - line.discount)) / PENNIES) *
			(100 + line.tax) / PENNIES

		//fmt.Printf(
		//	"o->lines:%d,partKey:%d,supp_num:%d,o->l[lcnt].suppkey:%d,o->l[lcnt].quantity:%d,o->totalprice:%d\n",
		//	len(order.lines),line.partKey, suppNum, line.suppKey, line.quantity,
		//	order.totalPrice)

		sDate := random(L_SDTE_MIN, L_SDTE_MAX, L_SDTE_SD)
		sDate += tmpDate

		cDate := random(L_CDTE_MIN, L_CDTE_MAX, L_CDTE_SD)
		cDate += tmpDate

		rDate := random(L_RDTE_MIN, L_RDTE_MAX, L_RDTE_SD)
		rDate += sDate
		line.sDate = ascDate[sDate-STARTDATE]
		line.cDate = ascDate[cDate-STARTDATE]
		line.rDate = ascDate[rDate-STARTDATE]

		if julian(int(rDate)) <= CURRENTDATE {
			var tmpStr string
			pickStr(&lRflagSet, L_RFLG_SD, &tmpStr)
			line.rFlag = tmpStr[0]
		} else {
			line.rFlag = "N"[0]
		}

		if julian(int(sDate)) <= CURRENTDATE {
			oCnt++
			line.lStatus = "F"[0]
		} else {
			line.lStatus = "O"[0]
		}

		order.lines = append(order.lines, line)
	}
	if oCnt > 0 {
		order.status = 'P'
	}
	if oCnt == len(order.lines) {
		order.status = 'F'
	}

	return order
}

func initOrder() {
	ockeyMin = 1
	ockeyMax = tDefs[CUST].base * scale
	ascDate = makeAscDate()
	odateMin = STARTDATE
	odateMax = STARTDATE + TOTDATE - (L_SDTE_MAX + L_RDTE_MAX) - 1
}
