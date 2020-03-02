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

const (
	deliverySelectNewOrder  = "SELECT no_o_id FROM new_order WHERE no_pk > ? ORDER BY no_pk LIMIT 1 FOR UPDATE"
	deliveryDeleteNewOrder  = `DELETE FROM new_order WHERE no_pk IN (?,?,?,?,?,?,?,?,?,?)`
	deliveryUpdateOrder     = `UPDATE orders SET o_carrier_id = ? WHERE o_pk IN (?,?,?,?,?,?,?,?,?,?)`
	deliverySelectOrders    = `SELECT o_d_id, o_c_id FROM orders WHERE o_pk IN (?,?,?,?,?,?,?,?,?,?)`
	deliveryUpdateOrderLine = `UPDATE order_line SET ol_delivery_d = ? WHERE 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ?`
	deliverySelectSumAmount = `SELECT ol_d_id, SUM(ol_amount) FROM order_line WHERE 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? AND ? OR 
ol_pk BETWEEN ? and ? GROUP BY ol_d_id`
	deliveryUpdateCustomer = `UPDATE customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_pk = ?`
)

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
	type deliveryOrder struct {
		oID    int
		cID    int
		amount float64
	}
	orders := make([]deliveryOrder, 10)
	for i := 0; i < districtPerWarehouse; i++ {
		if err = s.deliveryStmts[deliverySelectNewOrder].QueryRowContext(ctx, getNOPK(d.wID, i+1, 0)).Scan(&orders[i].oID); err == sql.ErrNoRows {
			continue
		} else if err != nil {
			return fmt.Errorf("exec %s failed %v", deliverySelectNewOrder, err)
		}
	}

	if _, err = s.deliveryStmts[deliveryDeleteNewOrder].ExecContext(ctx,
		getNOPK(d.wID, 1, orders[0].oID),
		getNOPK(d.wID, 2, orders[1].oID),
		getNOPK(d.wID, 3, orders[2].oID),
		getNOPK(d.wID, 4, orders[3].oID),
		getNOPK(d.wID, 5, orders[4].oID),
		getNOPK(d.wID, 6, orders[5].oID),
		getNOPK(d.wID, 7, orders[6].oID),
		getNOPK(d.wID, 8, orders[7].oID),
		getNOPK(d.wID, 9, orders[8].oID),
		getNOPK(d.wID, 10, orders[9].oID),
	); err != nil {
		return fmt.Errorf("exec %s failed %v", deliveryDeleteNewOrder, err)
	}

	if _, err = s.deliveryStmts[deliveryUpdateOrder].ExecContext(ctx, d.oCarrierID,
		getOPK(d.wID, 1, orders[0].oID),
		getOPK(d.wID, 2, orders[1].oID),
		getOPK(d.wID, 3, orders[2].oID),
		getOPK(d.wID, 4, orders[3].oID),
		getOPK(d.wID, 5, orders[4].oID),
		getOPK(d.wID, 6, orders[5].oID),
		getOPK(d.wID, 7, orders[6].oID),
		getOPK(d.wID, 8, orders[7].oID),
		getOPK(d.wID, 9, orders[8].oID),
		getOPK(d.wID, 10, orders[9].oID),
	); err != nil {
		return fmt.Errorf("exec %s failed %v", deliveryUpdateOrder, err)
	}

	if rows, err := s.deliveryStmts[deliverySelectOrders].QueryContext(ctx,
		getOPK(d.wID, 1, orders[0].oID),
		getOPK(d.wID, 2, orders[1].oID),
		getOPK(d.wID, 3, orders[2].oID),
		getOPK(d.wID, 4, orders[3].oID),
		getOPK(d.wID, 5, orders[4].oID),
		getOPK(d.wID, 6, orders[5].oID),
		getOPK(d.wID, 7, orders[6].oID),
		getOPK(d.wID, 8, orders[7].oID),
		getOPK(d.wID, 9, orders[8].oID),
		getOPK(d.wID, 10, orders[9].oID),
	); err != nil {
		return fmt.Errorf("exec %s failed %v", deliverySelectOrders, err)
	} else {
		for rows.Next() {
			var dID, cID int
			if err = rows.Scan(&dID, &cID); err != nil {
				return fmt.Errorf("exec %s failed %v", deliverySelectOrders, err)
			}
			orders[dID-1].cID = cID
		}
	}

	if _, err = s.deliveryStmts[deliveryUpdateOrderLine].ExecContext(ctx, time.Now().Format(timeFormat),
		getOLPK(d.wID, 1, orders[0].oID, 0), getOLPK(d.wID, 1, orders[0].oID, 15),
		getOLPK(d.wID, 2, orders[0].oID, 0), getOLPK(d.wID, 2, orders[0].oID, 15),
		getOLPK(d.wID, 3, orders[0].oID, 0), getOLPK(d.wID, 3, orders[0].oID, 15),
		getOLPK(d.wID, 4, orders[0].oID, 0), getOLPK(d.wID, 4, orders[0].oID, 15),
		getOLPK(d.wID, 5, orders[0].oID, 0), getOLPK(d.wID, 5, orders[0].oID, 15),
		getOLPK(d.wID, 6, orders[0].oID, 0), getOLPK(d.wID, 6, orders[0].oID, 15),
		getOLPK(d.wID, 7, orders[0].oID, 0), getOLPK(d.wID, 7, orders[0].oID, 15),
		getOLPK(d.wID, 8, orders[0].oID, 0), getOLPK(d.wID, 8, orders[0].oID, 15),
		getOLPK(d.wID, 9, orders[0].oID, 0), getOLPK(d.wID, 9, orders[0].oID, 15),
		getOLPK(d.wID, 10, orders[0].oID, 0), getOLPK(d.wID, 10, orders[0].oID, 15),
	); err != nil {
		return fmt.Errorf("exec %s failed %v", deliveryUpdateOrderLine, err)
	}

	if rows, err := s.deliveryStmts[deliverySelectSumAmount].QueryContext(ctx,
		getOLPK(d.wID, 1, orders[0].oID, 0), getOLPK(d.wID, 1, orders[0].oID, 15),
		getOLPK(d.wID, 2, orders[0].oID, 0), getOLPK(d.wID, 2, orders[0].oID, 15),
		getOLPK(d.wID, 3, orders[0].oID, 0), getOLPK(d.wID, 3, orders[0].oID, 15),
		getOLPK(d.wID, 4, orders[0].oID, 0), getOLPK(d.wID, 4, orders[0].oID, 15),
		getOLPK(d.wID, 5, orders[0].oID, 0), getOLPK(d.wID, 5, orders[0].oID, 15),
		getOLPK(d.wID, 6, orders[0].oID, 0), getOLPK(d.wID, 6, orders[0].oID, 15),
		getOLPK(d.wID, 7, orders[0].oID, 0), getOLPK(d.wID, 7, orders[0].oID, 15),
		getOLPK(d.wID, 8, orders[0].oID, 0), getOLPK(d.wID, 8, orders[0].oID, 15),
		getOLPK(d.wID, 9, orders[0].oID, 0), getOLPK(d.wID, 9, orders[0].oID, 15),
		getOLPK(d.wID, 10, orders[0].oID, 0), getOLPK(d.wID, 10, orders[0].oID, 15),
	); err != nil {
		return fmt.Errorf("exec %s failed %v", deliverySelectSumAmount, err)
	} else {
		for rows.Next() {
			var dID int
			var amount float64
			if err = rows.Scan(&dID, &amount); err != nil {
				return fmt.Errorf("exec %s failed %v", deliverySelectOrders, err)
			}
			orders[dID-1].amount = amount
		}
	}

	for i := 0; i < districtPerWarehouse; i++ {
		order := &orders[i]
		if order.oID == 0 {
			continue
		}
		if _, err = s.deliveryStmts[deliveryUpdateCustomer].ExecContext(ctx, order.amount, getCPK(d.wID, i+1, order.cID)); err != nil {
			return fmt.Errorf("exec %s failed %v", deliveryUpdateCustomer, err)
		}
	}
	return tx.Commit()
}
