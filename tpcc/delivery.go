package tpcc

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type deliveryData struct {
	wID         int
	oCarrierID  int
	olDeliveryD string
}

func (w *Workloader) runDelivery(ctx context.Context, thread int) error {
	s := w.getState(ctx)

	d := deliveryData{
		wID:        randInt(s.R, 1, w.cfg.Warehouses),
		oCarrierID: randInt(s.R, 1, 10),
	}

	tx, err := w.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i := 0; i < districtPerWarehouse; i++ {
		dID := i + 1
		// SELECT no_o_id
		//  FROM new_order
		//  WHERE no_d_id = :d_id AND no_w_id = :w_id ORDER BY no_o_id ASC;
		var noOID int
		query := "SELECT no_o_id FROM new_order WHERE no_d_id = ? AND no_w_id = ? ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE"
		if err := tx.QueryRowContext(ctx, query, dID, d.wID).Scan(&noOID); err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return fmt.Errorf("Exec %s failed %v", query, err)
		}

		// DELETE FROM new_order WHERE CURRENT OF c_no
		query = "DELETE FROM new_order WHERE no_o_id = ? AND no_d_id = ? AND no_w_id = ?"
		if _, err := tx.ExecContext(ctx, query, noOID, dID, d.wID); err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}

		// SELECT o_c_id INTO :c_id FROM orders WHERE o_id = :no_o_id AND o_d_id = :d_id AND
		// o_w_id = :w_id;
		var oCID int
		query = "SELECT o_c_id FROM orders WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?"
		if err := tx.QueryRowContext(ctx, query, noOID, dID, d.wID).Scan(&oCID); err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}

		// UPDATE orders SET o_carrier_id = :o_carrier_id WHERE o_id = :no_o_id AND o_d_id = :d_id AND
		//  o_w_id = :w_id;
		query = "UPDATE orders SET o_carrier_id = ? WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?"
		if _, err := tx.ExecContext(ctx, query, d.oCarrierID, noOID, dID, d.wID); err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}

		// UPDATE order_line SET ol_delivery_d = :datetime WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND
		// 	ol_w_id = :w_id;
		query = "UPDATE order_line SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?"
		if _, err := tx.ExecContext(ctx, query, time.Now().Format(timeFormat), noOID, dID, d.wID); err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}

		// SELECT SUM(ol_amount) INTO :ol_total FROM order_line
		// 	WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND ol_w_id = :w_id;
		var olTotal float64
		query = "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?"
		if err := tx.QueryRowContext(ctx, query, noOID, dID, d.wID).Scan(&olTotal); err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}

		// UPDATE customer SET c_balance = c_balance + :ol_total WHERE c_id = :c_id AND c_d_id = :d_id AND
		// 	c_w_id = :w_id;

		query = "UPDATE customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?"
		if _, err := tx.ExecContext(ctx, query, olTotal, oCID, dID, d.wID); err != nil {
			return fmt.Errorf("Exec %s failed %v", query, err)
		}
	}

	return tx.Commit()
}
