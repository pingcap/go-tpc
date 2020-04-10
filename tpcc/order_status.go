package tpcc

import (
	"context"
	"database/sql"
	"fmt"
)

const (
	orderStatusSelectCustomerByLast = `SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first`
	orderStatusSelectCustomerByID   = `SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_pk = ?`
	orderStatusSelectLatestOrder    = `SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1`
	orderStatusSelectOrderLine      = `SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_pk BETWEEN ? AND ?`
)

type orderStatusData struct {
	wID int
	dID int

	cID      int
	cLast    string
	cBalance float64
	cFirst   string
	cMiddle  string

	oID        int
	oEntryD    string
	oCarrierID sql.NullInt64
}

func (w *Workloader) runOrderStatus(ctx context.Context, thread int) error {
	s := w.getState(ctx)
	d := orderStatusData{
		wID: randInt(s.R, 1, w.cfg.Warehouses),
		dID: randInt(s.R, 1, districtPerWarehouse),
	}

	// refer 2.6.1.2
	if s.R.Intn(100) < 60 {
		d.cLast = randCLast(s.R, s.Buf)
	} else {
		d.cID = randCustomerID(s.R)
	}

	tx, err := w.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if d.cID == 0 {
		// by name
		rows, err := s.orderStatusStmts[orderStatusSelectCustomerByLast].QueryContext(ctx, d.wID, d.dID, d.cLast)
		if err != nil {
			return fmt.Errorf("exec %s failed %v", orderStatusSelectCustomerByLast, err)
		}
		var ids []int
		for rows.Next() {
			var id int
			if err = rows.Scan(&id); err != nil {
				return fmt.Errorf("exec %s failed %v", orderStatusSelectCustomerByLast, err)
			}
			ids = append(ids, id)
		}
		if len(ids) == 0 {
			return fmt.Errorf("customer for (%d, %d, %s) not found", d.wID, d.dID, d.cLast)
		}
		d.cID = ids[(len(ids)+1)/2-1]
	} else {
		if err := s.orderStatusStmts[orderStatusSelectCustomerByID].QueryRowContext(ctx, getCPK(d.wID, d.dID, d.cID)).Scan(&d.cBalance, &d.cFirst, &d.cMiddle, &d.cLast); err != nil {
			return fmt.Errorf("exec %s failed %v", orderStatusSelectCustomerByID, err)
		}
	}

	// SELECT o_id, o_carrier_id, o_entry_d
	//  INTO :o_id, :o_carrier_id, :entdate FROM orders
	//  ORDER BY o_id DESC;

	// refer 2.6.2.2 - select the latest order
	if err := s.orderStatusStmts[orderStatusSelectLatestOrder].QueryRowContext(ctx, d.wID, d.dID, d.cID).Scan(&d.oID, &d.oCarrierID, &d.oEntryD); err != nil {
		return fmt.Errorf("exec %s failed %v", orderStatusSelectLatestOrder, err)
	}

	// SQL DECLARE c_line CURSOR FOR SELECT ol_i_id, ol_supply_w_id, ol_quantity,
	// 	ol_amount, ol_delivery_d
	// 	FROM order_line
	// 	WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
	// OPEN c_line;
	rows, err := s.orderStatusStmts[orderStatusSelectOrderLine].QueryContext(ctx, getOLPK(d.wID, d.dID, d.oID, 0), getOLPK(d.wID, d.dID, d.oID+1, 0))
	if err != nil {
		return fmt.Errorf("exec %s failed %v", orderStatusSelectOrderLine, err)
	}
	defer rows.Close()

	items := make([]orderItem, 0, 4)
	for rows.Next() {
		var item orderItem
		if err := rows.Scan(&item.olIID, &item.olSupplyWID, &item.olQuantity, &item.olAmount, &item.olDeliveryD); err != nil {
			return err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return tx.Commit()
}
