package tpcc

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/siddontang/go-tpc/pkg/load"
)

const (
	maxItems             = 100000
	stockPerWarehouse    = 100000
	districtPerWarehouse = 10
	customerPerDistrict  = 3000
	orderPerDistrict     = 3000
	newOrderPerDistrict  = 900

	timeFormat = "2006-01-02 15:04:05"
)

func (w *Workloader) loadItem(ctx context.Context, tableID int) error {
	fmt.Printf("load to item%d\n", tableID)
	s := w.getState(ctx)
	hint := fmt.Sprintf("INSERT INTO item%d (i_id, i_im_id, i_name, i_price, i_data) VALUES ", tableID)
	l := load.NewBatchLoader(s.Conn, hint)

	for i := 0; i < maxItems; i++ {
		s.Buf.Reset()

		iImID := randInt(s.R, 1, 10000)
		iPrice := float64(randInt(s.R, 100, 10000)) / float64(100.0)
		iName := randChars(s.R, s.Buf, 14, 24)
		iData := randOriginalString(s.R, s.Buf)

		v := fmt.Sprintf(`(%d, %d, '%s', %f, '%s')`, i+1, iImID, iName, iPrice, iData)

		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}

	return l.Flush(ctx)
}

func (w *Workloader) loadWarhouse(ctx context.Context, tableID int, warehouse int) error {
	fmt.Printf("load to warehouse%d in warehouse %d\n", tableID, warehouse)
	s := w.getState(ctx)
	hint := fmt.Sprintf("INSERT INTO warehouse%d (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES ", tableID)
	l := load.NewBatchLoader(s.Conn, hint)

	wName := randChars(s.R, s.Buf, 6, 10)
	wStree1 := randChars(s.R, s.Buf, 10, 20)
	wStree2 := randChars(s.R, s.Buf, 10, 20)
	wCity := randChars(s.R, s.Buf, 10, 20)
	wState := randState(s.R, s.Buf)
	wZip := randZip(s.R, s.Buf)
	wTax := randTax(s.R)
	wYtd := 300000.00

	v := fmt.Sprintf(`(%d, '%s', '%s', '%s', '%s', '%s', '%s', %f, %f)`,
		warehouse, wName, wStree1, wStree2, wCity, wState, wZip, wTax, wYtd)
	l.InsertValue(ctx, v)

	return l.Flush(ctx)
}

func (w *Workloader) loadStock(ctx context.Context, tableID int, warehouse int) error {
	fmt.Printf("load to stock%d in warehouse %d\n", tableID, warehouse)

	s := w.getState(ctx)

	hint := fmt.Sprintf(`INSERT INTO stock%d (s_i_id, s_w_id, s_quantity, 
s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, 
s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES `, tableID)

	l := load.NewBatchLoader(s.Conn, hint)

	for i := 0; i < stockPerWarehouse; i++ {
		s.Buf.Reset()

		sIID := i + 1
		sWID := warehouse
		sQuantity := randInt(s.R, 10, 100)
		sDist01 := randLetters(s.R, s.Buf, 24, 24)
		sDist02 := randLetters(s.R, s.Buf, 24, 24)
		sDist03 := randLetters(s.R, s.Buf, 24, 24)
		sDist04 := randLetters(s.R, s.Buf, 24, 24)
		sDist05 := randLetters(s.R, s.Buf, 24, 24)
		sDist06 := randLetters(s.R, s.Buf, 24, 24)
		sDist07 := randLetters(s.R, s.Buf, 24, 24)
		sDist08 := randLetters(s.R, s.Buf, 24, 24)
		sDist09 := randLetters(s.R, s.Buf, 24, 24)
		sDist10 := randLetters(s.R, s.Buf, 24, 24)
		sYtd := 0
		sOrderCnt := 0
		sRemoteCnt := 0
		sData := randOriginalString(s.R, s.Buf)

		v := fmt.Sprintf(`(%d, %d, %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, '%s')`,
			sIID, sWID, sQuantity, sDist01, sDist02, sDist03, sDist04, sDist05, sDist06, sDist07, sDist08, sDist09, sDist10, sYtd, sOrderCnt, sRemoteCnt, sData)
		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}
	return l.Flush(ctx)
}

func (w *Workloader) loadDistrict(ctx context.Context, tableID int, warehouse int) error {
	fmt.Printf("load to district%d in warehouse %d\n", tableID, warehouse)

	s := w.getState(ctx)

	hint := fmt.Sprintf(`INSERT INTO district%d (d_id, d_w_id, d_name, d_street_1, d_street_2, 
d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES `, tableID)

	l := load.NewBatchLoader(s.Conn, hint)

	for i := 0; i < districtPerWarehouse; i++ {
		s.Buf.Reset()

		dID := i + 1
		dWID := warehouse
		dName := randChars(s.R, s.Buf, 6, 10)
		dStreet1 := randChars(s.R, s.Buf, 10, 20)
		dStreet2 := randChars(s.R, s.Buf, 10, 20)
		dCity := randChars(s.R, s.Buf, 10, 20)
		dState := randState(s.R, s.Buf)
		dZip := randZip(s.R, s.Buf)
		dTax := randTax(s.R)
		dYtd := 300000.00
		dNextOID := 3001

		v := fmt.Sprintf(`(%d, %d, '%s', '%s', '%s', '%s', '%s', '%s', %f, %f, %d)`, dID, dWID,
			dName, dStreet1, dStreet2, dCity, dState, dZip, dTax, dYtd, dNextOID)

		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}
	return l.Flush(ctx)
}

func (w *Workloader) loadCustomer(ctx context.Context, tableID int, warehouse int, district int) error {
	fmt.Printf("load to customer%d in warehouse %d district %d\n", tableID, warehouse, district)

	s := w.getState(ctx)

	hint := fmt.Sprintf(`INSERT INTO customer%d (c_id, c_d_id, c_w_id, c_last, c_middle, c_first, 
c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim,
c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) VALUES `, tableID)

	l := load.NewBatchLoader(s.Conn, hint)

	for i := 0; i < customerPerDistrict; i++ {
		s.Buf.Reset()

		cID := i + 1
		cDID := district
		cWID := warehouse
		var cLast string
		if i < 1000 {
			cLast = randCLastSyllables(i, s.Buf)
		} else {
			cLast = randCLast(s.R, s.Buf)
		}
		cMiddle := "OE"
		cFirst := randChars(s.R, s.Buf, 8, 16)
		cStreet1 := randChars(s.R, s.Buf, 10, 20)
		cStreet2 := randChars(s.R, s.Buf, 10, 20)
		cCity := randChars(s.R, s.Buf, 10, 20)
		cState := randState(s.R, s.Buf)
		cZip := randZip(s.R, s.Buf)
		cPhone := randNumbers(s.R, s.Buf, 16, 16)
		cSince := w.initLoadTime
		cCredit := "GC"
		if s.R.Intn(10) == 0 {
			cCredit = "BC"
		}
		cCreditLim := 50000.00
		cDisCount := float64(randInt(s.R, 0, 5000)) / float64(10000.0)
		cBalance := -10.00
		cYtdPayment := 10.00
		cPaymentCnt := 1
		cDeliveryCnt := 0
		cData := randChars(s.R, s.Buf, 300, 500)

		v := fmt.Sprintf(`(%d, %d, %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %f, %f, %f, %f, %d, %d, '%s')`,
			cID, cDID, cWID, cLast, cMiddle, cFirst, cStreet1, cStreet2, cCity, cState,
			cZip, cPhone, cSince, cCredit, cCreditLim, cDisCount, cBalance,
			cYtdPayment, cPaymentCnt, cDeliveryCnt, cData)
		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}

	return l.Flush(ctx)
}

func (w *Workloader) loadHistory(ctx context.Context, tableID int, warehouse int, district int) error {
	fmt.Printf("load to history%d in warehouse %d district %d\n", tableID, warehouse, district)

	s := w.getState(ctx)

	hint := fmt.Sprintf(`INSERT INTO history%d (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES `, tableID)

	l := load.NewBatchLoader(s.Conn, hint)

	// 1 customer has 1 row
	for i := 0; i < customerPerDistrict; i++ {
		s.Buf.Reset()

		hCID := i + 1
		hCDID := district
		hCWID := warehouse
		hDID := district
		hWID := warehouse
		hDate := w.initLoadTime
		hAmount := 10.00
		hData := randChars(s.R, s.Buf, 12, 24)

		v := fmt.Sprintf(`(%d, %d, %d, %d, %d, '%s', %f, '%s')`,
			hCID, hCDID, hCWID, hDID, hWID, hDate, hAmount, hData)
		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}
	return l.Flush(ctx)
}

func (w *Workloader) loadOrder(ctx context.Context, tableID int, warehouse int, district int) ([]int, error) {
	fmt.Printf("load to order%d in warehouse %d district %d\n", tableID, warehouse, district)

	s := w.getState(ctx)

	hint := fmt.Sprintf(`INSERT INTO order%d (o_id, o_c_id, o_d_id, o_w_id, o_entry_d, 
o_carrier_id, o_ol_cnt, o_all_local) VALUES `, tableID)

	l := load.NewBatchLoader(s.Conn, hint)

	cids := rand.Perm(orderPerDistrict)
	s.R.Shuffle(len(cids), func(i, j int) {
		cids[i], cids[j] = cids[j], cids[i]
	})
	olCnts := make([]int, orderPerDistrict)
	for i := 0; i < orderPerDistrict; i++ {
		s.Buf.Reset()

		oID := i + 1
		oCID := cids[i] + 1
		oDID := district
		oWID := warehouse
		oEntryD := w.initLoadTime
		oCarrierID := "NULL"
		if oID < 2101 {
			oCarrierID = strconv.FormatInt(int64(randInt(s.R, 1, 10)), 10)
		}
		oOLCnt := randInt(s.R, 5, 15)
		olCnts[i] = oOLCnt
		oAllLocal := 1

		v := fmt.Sprintf(`(%d, %d, %d, %d, '%s', %s, %d, %d)`, oID, oCID, oDID, oWID, oEntryD, oCarrierID, oOLCnt, oAllLocal)
		if err := l.InsertValue(ctx, v); err != nil {
			return nil, err
		}
	}

	return olCnts, l.Flush(ctx)
}

func (w *Workloader) loadNewOrder(ctx context.Context, tableID int, warehouse int, district int) error {
	fmt.Printf("load to new_order%d in warehouse %d district %d\n", tableID, warehouse, district)

	s := w.getState(ctx)

	hint := fmt.Sprintf(`INSERT INTO new_order%d (no_o_id, no_d_id, no_w_id) VALUES `, tableID)

	l := load.NewBatchLoader(s.Conn, hint)

	for i := 0; i < newOrderPerDistrict; i++ {
		s.Buf.Reset()

		noOID := 2101 + i
		noDID := district
		noWID := warehouse

		v := fmt.Sprintf(`(%d, %d, %d)`, noOID, noDID, noWID)
		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}

	return l.Flush(ctx)
}

func (w *Workloader) loadOrderLine(ctx context.Context, tableID int, warehouse int, district int, olCnts []int) error {
	fmt.Printf("load to order_line%d in warehouse %d district %d\n", tableID, warehouse, district)

	s := w.getState(ctx)

	hint := fmt.Sprintf(`INSERT INTO order_line%d (ol_o_id, ol_d_id, ol_w_id, ol_number,
ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES `, tableID)

	l := load.NewBatchLoader(s.Conn, hint)

	for i := 0; i < orderPerDistrict; i++ {
		for j := 0; j < olCnts[i]; j++ {
			s.Buf.Reset()

			olOID := i + 1
			olDID := district
			olWID := warehouse
			olNumber := j + 1
			olIID := randInt(s.R, 1, 100000)
			olSupplyWID := warehouse
			olDeliveryD := "NULL"
			if olOID < 2101 {
				olDeliveryD = fmt.Sprintf(`'%s'`, w.initLoadTime)
			}
			olQuantity := 5
			olAmount := 0.00
			if olOID < 2101 {
				olAmount = float64(randInt(s.R, 1, 999999)) / 100.0
			}
			olDistInfo := randChars(s.R, s.Buf, 24, 24)
			v := fmt.Sprintf(`(%d, %d, %d, %d, %d, %d, %s, %d, %f, '%s')`,
				olOID, olDID, olWID, olNumber, olIID, olSupplyWID,
				olDeliveryD, olQuantity, olAmount, olDistInfo)
			if err := l.InsertValue(ctx, v); err != nil {
				return err
			}
		}
	}

	return l.Flush(ctx)
}
