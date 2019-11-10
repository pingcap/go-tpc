package tpcc

import (
	"context"
	"database/sql"
	"fmt"
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
		// SELECT count(c_id) INTO :namecnt FROM customer
		//	WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id
		var nameCnt int
		query := `SELECT count(c_id) namecnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?`
		if err := tx.QueryRowContext(ctx, query, d.wID, d.dID, d.cLast).Scan(&nameCnt); err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}

		// DECLARE c_name CURSOR FOR SELECT c_balance, c_first, c_middle, c_id
		// 	FROM customer WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id ORDER BY c_first;
		// OPEN c_name;
		if nameCnt%2 == 1 {
			nameCnt += 1
		}

		query = `SELECT c_balance, c_first, c_middle, c_id FROM customer
WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first`
		rows, err := tx.QueryContext(ctx, query, d.wID, d.dID, d.cLast)
		if err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}
		for i := 0; i < nameCnt/2 && rows.Next(); i++ {
			if err := rows.Scan(&d.cBalance, &d.cFirst, &d.cMiddle, &d.cID); err != nil {
				return err
			}
		}

		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	} else {
		// SELECT c_balance, c_first, c_middle, c_last
		//  INTO :c_balance, :c_first, :c_middle, :c_last
		//  FROM customer
		//  WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
		query := `SELECT c_balance, c_first, c_middle, c_last FROM customer 
WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?`
		if err := tx.QueryRowContext(ctx, query, d.cID, d.dID, d.wID).Scan(&d.cBalance, &d.cFirst, &d.cMiddle, &d.cLast); err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}
	}

	// SELECT o_id, o_carrier_id, o_entry_d
	//  INTO :o_id, :o_carrier_id, :entdate FROM orders
	//  ORDER BY o_id DESC;

	// refer 2.6.2.2 - select the latest order
	query := `SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id = ?
AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1`
	if err := tx.QueryRowContext(ctx, query, d.wID, d.dID, d.cID).Scan(&d.oID, &d.oCarrierID, &d.oEntryD); err != nil {
		return fmt.Errorf("Exec %s failed %v", query, err)
	}

	// SQL DECLARE c_line CURSOR FOR SELECT ol_i_id, ol_supply_w_id, ol_quantity,
	// 	ol_amount, ol_delivery_d
	// 	FROM order_line
	// 	WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
	// OPEN c_line;
	query = `SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
FROM order_line WHERE ol_w_id = ? AND ol_d_id = ?  AND ol_o_id = ?`
	rows, err := tx.QueryContext(ctx, query, d.wID, d.dID, d.oID)
	if err != nil {
		return fmt.Errorf("Exec %s failed %v", query, err)
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
