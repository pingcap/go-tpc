package dbgen

import (
	"fmt"
	"io"
)

const (
	oLcntMin = 1
	oLcntMax = 7
	oSuppSd  = 10
	oClrkSd  = 11
	oOdateSd = 13
	oPrioSd  = 38
	oCkeySd  = 40
	oLcntSd  = 43
	oCmntLen = 49
	oCmntSd  = 12
	oClrkScl = 1000

	lQtySd        = 14
	lDcntSd       = 15
	lTaxSd        = 16
	lShipSd       = 17
	lSmodeSd      = 18
	lPkeySd       = 19
	lSkeySd       = 20
	lSdteSd       = 21
	lCdteSd       = 22
	lRdteSd       = 23
	lRflgSd       = 24
	lCmntLen      = 27
	lCmntSd       = 25
	pennies       = 100
	ordersPerCust = 10
)

var (
	ockeyMin dssHuge
	ockeyMax dssHuge
	odateMin dssHuge
	odateMax dssHuge
	ascDate  []string
)

type Order struct {
	OKey          dssHuge
	CustKey       dssHuge
	Status        byte
	TotalPrice    dssHuge
	Date          string
	OrderPriority string
	Clerk         string
	ShipPriority  int64
	Comment       string
	Lines         []LineItem
}

type orderLoader struct {
	io.StringWriter
}

func (o orderLoader) Load(item interface{}) error {
	order := item.(*Order)
	if _, err := o.WriteString(
		fmt.Sprintf("%d|%d|%c|%s|%s|%s|%s|%d|%s|\n",
			order.OKey,
			order.CustKey,
			order.Status,
			FmtMoney(order.TotalPrice),
			order.Date,
			order.OrderPriority,
			order.Clerk,
			order.ShipPriority,
			order.Comment)); err != nil {
		return err
	}
	return nil
}
func (o orderLoader) Flush() error {
	return nil
}

func newOrderLoader(writer io.StringWriter) orderLoader {
	return orderLoader{writer}
}

func sdOrder(child Table, skipCount dssHuge) {
	advanceStream(oLcntSd, skipCount, false)
	advanceStream(oCkeySd, skipCount, false)
	advanceStream(oCmntSd, skipCount*2, false)
	advanceStream(oSuppSd, skipCount, false)
	advanceStream(oClrkSd, skipCount, false)
	advanceStream(oPrioSd, skipCount, false)
	advanceStream(oOdateSd, skipCount, false)
}

func makeOrder(idx dssHuge) *Order {
	delta := 1
	order := &Order{}
	order.OKey = makeSparse(idx)
	if scale >= 30000 {
		order.CustKey = random64(ockeyMin, ockeyMax, oCkeySd)
	} else {
		order.CustKey = random(ockeyMin, ockeyMax, oCkeySd)
	}

	// Comment: Orders are not present for all customers.
	// In fact, one-third of the customers do not have any order in the database.
	// The orders are assigned at random to two-thirds of the customers
	for order.CustKey%3 == 0 {
		order.CustKey += dssHuge(delta)
		order.CustKey = min(order.CustKey, ockeyMax)
		delta *= -1
	}
	tmpDate := random(odateMin, odateMax, oOdateSd)
	order.Date = ascDate[tmpDate-startDate]
	pickStr(&oPrioritySet, oPrioSd, &order.OrderPriority)
	order.Clerk = pickClerk()
	order.Comment = makeText(oCmntLen, oCmntSd)
	order.ShipPriority = 0
	order.TotalPrice = 0
	order.Status = 'O'
	oCnt := 0
	lineCount := random(oLcntMin, oLcntMax, oLcntSd)

	for lCnt := dssHuge(0); lCnt < lineCount; lCnt++ {
		line := LineItem{}
		line.OKey = order.OKey
		line.LCnt = lCnt + 1
		line.Quantity = random(lQtyMin, lQtyMax, lQtySd)
		line.Discount = random(lDcntMin, lDcntMax, lDcntSd)
		line.Tax = random(lTaxMin, lTaxMax, lTaxSd)

		pickStr(&lInstructSet, lShipSd, &line.ShipInstruct)
		pickStr(&lSmodeSet, lSmodeSd, &line.ShipMode)
		line.Comment = makeText(lCmntLen, lCmntSd)

		if scale > 30000 {
			line.PartKey = random64(lPkeyMin, LPkeyMax, lPkeySd)
		} else {
			line.PartKey = random(lPkeyMin, LPkeyMax, lPkeySd)
		}

		rPrice := rpbRoutine(line.PartKey)
		suppNum := random(0, 3, lSkeySd)
		line.SuppKey = partSuppBridge(line.PartKey, suppNum)
		line.EPrice = rPrice * line.Quantity

		order.TotalPrice += ((line.EPrice * (100 - line.Discount)) / pennies) *
			(100 + line.Tax) / pennies

		sDate := random(lSdteMin, lSdteMax, lSdteSd)
		sDate += tmpDate

		cDate := random(lCdteMin, lCdteMax, lCdteSd)
		cDate += tmpDate

		rDate := random(lRdteMin, lRdteMax, lRdteSd)
		rDate += sDate
		line.SDate = ascDate[sDate-startDate]
		line.CDate = ascDate[cDate-startDate]
		line.RDate = ascDate[rDate-startDate]

		if julian(int(rDate)) <= currentDate {
			var tmpStr string
			pickStr(&lRflagSet, lRflgSd, &tmpStr)
			line.RFlag = tmpStr[0]
		} else {
			line.RFlag = "N"[0]
		}

		if julian(int(sDate)) <= currentDate {
			oCnt++
			line.LStatus = "F"[0]
		} else {
			line.LStatus = "O"[0]
		}

		order.Lines = append(order.Lines, line)
	}
	if oCnt > 0 {
		order.Status = 'P'
	}
	if oCnt == len(order.Lines) {
		order.Status = 'F'
	}

	return order
}

func initOrder() {
	ockeyMin = 1
	ockeyMax = tDefs[TCust].base * scale
	ascDate = makeAscDate()
	odateMin = startDate
	odateMax = startDate + totDate - (lSdteMax + lRdteMax) - 1
}
