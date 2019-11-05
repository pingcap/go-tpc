package tpcc

import (
	"context"
	"fmt"

	"github.com/siddontang/go-tpc/pkg/load"
)

const (
	maxItems              = 100000
	stockPerWarehouse     = 100000
	districtPerWarehouse  = 10
	customerPerWarehouse  = 30000
	customerPerDistrict   = 3000
	orderPerWarehouse     = 30000
	historyPerWarehouse   = 30000
	newOrderPerWarehouse  = 9000
	orderLinePerWarehouse = 300000
	minOrderLinePerOrder  = 5
	maxOrderLinePerOrder  = 15
)

func (w *Workloader) loadItem(ctx context.Context, tableID int) error {
	s := w.base.GetState(ctx)
	hint := fmt.Sprintf("INSERT INTO item%d (i_id, i_im_id, i_name, i_price, i_data) VALUES ", tableID)
	l := load.NewBatchLoader(s.Conn, hint)

	for i := 0; i < maxItems; i++ {
		s.Buf.Reset()
		i_im_id := randInt(s.R, 1, 10000)
		i_price := float64(randInt(s.R, 100, 10000)) / float64(100.0)
		i_name := randChars(s.R, s.Buf, 14, 24)
		i_data := randOriginalString(s.R, s.Buf)

		v := fmt.Sprintf(`(%d, %d, '%s', %f, '%s')`, i+1, i_im_id, i_name, i_price, i_data)

		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}

	return l.Flush(ctx)
}

func (w *Workloader) loadWarhouse(ctx context.Context, tableID int, warehouse int) error {
	s := w.base.GetState(ctx)
	hint := fmt.Sprintf("INSERT INTO warehouse%d (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES ", tableID)
	l := load.NewBatchLoader(s.Conn, hint)

	w_name := randChars(s.R, s.Buf, 6, 10)
	w_stree1 := randChars(s.R, s.Buf, 10, 20)
	w_stree2 := randChars(s.R, s.Buf, 10, 20)
	w_city := randChars(s.R, s.Buf, 10, 20)
	w_state := randState(s.R, s.Buf)
	w_zip := randZip(s.R, s.Buf)
	w_tax := randTax(s.R)
	w_ytd := 300000.00

	v := fmt.Sprintf(`(%d, '%s', '%s', '%s', '%s', '%s', '%s', %f, %f)`,
		warehouse, w_name, w_stree1, w_stree2, w_city, w_state, w_zip, w_tax, w_ytd)
	l.InsertValue(ctx, v)

	return l.Flush(ctx)
}

func (w *Workloader) loadStock(ctx context.Context, tableID int, warehouse int) error {
	s := w.base.GetState(ctx)

	hint := fmt.Sprintf(`INSERT INTO stock%d (s_i_id, s_w_id, s_quantity, 
s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, 
s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES `, tableID)

	l := load.NewBatchLoader(s.Conn, hint)

	for i := 0; i < stockPerWarehouse; i++ {
		s.Buf.Reset()
		s_i_id := i + 1
		s_w_id := warehouse
		s_quantity := randInt(s.R, 10, 100)
		s_dist_01 := randLetters(s.R, s.Buf, 24, 24)
		s_dist_02 := randLetters(s.R, s.Buf, 24, 24)
		s_dist_03 := randLetters(s.R, s.Buf, 24, 24)
		s_dist_04 := randLetters(s.R, s.Buf, 24, 24)
		s_dist_05 := randLetters(s.R, s.Buf, 24, 24)
		s_dist_06 := randLetters(s.R, s.Buf, 24, 24)
		s_dist_07 := randLetters(s.R, s.Buf, 24, 24)
		s_dist_08 := randLetters(s.R, s.Buf, 24, 24)
		s_dist_09 := randLetters(s.R, s.Buf, 24, 24)
		s_dist_10 := randLetters(s.R, s.Buf, 24, 24)
		s_ytd := 0
		s_order_cnt := 0
		s_remote_cnt := 0
		s_data := randOriginalString(s.R, s.Buf)

		v := fmt.Sprintf(`(%d, %d, %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, '%s')`,
			s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07,
			s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data)
		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}
	return l.Flush(ctx)
}

func (w *Workloader) loadDistrict(ctx context.Context, tableID int, warehouse int) error {
	s := w.base.GetState(ctx)

	hint := fmt.Sprintf(`INSERT INTO district%d (d_id, d_w_id, d_name, d_street_1, d_street_2, 
d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES `, tableID)

	l := load.NewBatchLoader(s.Conn, hint)

	for i := 0; i < districtPerWarehouse; i++ {
		d_id := i + 1
		d_w_id := warehouse
		d_name := randChars(s.R, s.Buf, 6, 10)
		d_street1 := randChars(s.R, s.Buf, 10, 20)
		d_street2 := randChars(s.R, s.Buf, 10, 20)
		d_city := randChars(s.R, s.Buf, 10, 20)
		d_state := randState(s.R, s.Buf)
		d_zip := randZip(s.R, s.Buf)
		d_tax := randTax(s.R)
		d_ytd := 300000.00
		d_next_o_id := 3001

		v := fmt.Sprintf(`(%d, %d, '%s', '%s', '%s', '%s', '%s', '%s', %f, %f, %d)`, d_id, d_w_id,
			d_name, d_street1, d_street2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id)

		if err := l.InsertValue(ctx, v); err != nil {
			return err
		}
	}
	return l.Flush(ctx)
}

func (w *Workloader) loadCustomer(ctx context.Context, tableID int, warehouse int, district int) error {

	return nil
}

func (w *Workloader) loadHistory(ctx context.Context, tableID int, warehouse int, district int, customer int) error {
	return nil
}

func (w *Workloader) loadOrder(ctx context.Context, tableID int, warehouse int, district int) error {
	return nil
}

func (w *Workloader) loadOrderLine(ctx context.Context, tableID int, warehouse int, district int, order int) error {
	return nil
}

func (w *Workloader) loadNewOrder(ctx context.Context, tableID int, warehouse int, district int) error {
	return nil
}
