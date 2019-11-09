package tpcc

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"
)

func (w *Workloader) otherWarehouse(ctx context.Context, warehouse int) int {
	s := w.getState(ctx)

	if w.cfg.Warehouses == 1 {
		return warehouse
	}

	var other int
	for {
		other = randInt(s.R, 1, w.cfg.Warehouses)
		if other != warehouse {
			break
		}
	}
	return other
}

type orderItem struct {
	olSupplyWID int
	olIID       int
	olNumber    int
	olQuantity  int
	olAmount    float64
	iPrice      float64
	iName       string
	iData       string
}

type newOrderData struct {
	wID    int
	dID    int
	cID    int
	oOlCnt int

	cDiscount float64
	cLast     string
	cCredit   []byte
	wTax      float64

	dNextOID int
	dTax     float64
}

func (w *Workloader) runNewOrder(ctx context.Context, thread int) error {
	s := w.getState(ctx)

	// refer 2.4.1
	d := newOrderData{
		wID:    randInt(s.R, 1, w.cfg.Warehouses),
		dID:    randInt(s.R, 1, 10),
		cID:    randCustomerID(s.R),
		oOlCnt: randInt(s.R, 5, 15),
	}

	rbk := randInt(s.R, 1, 100)
	allLocal := 1

	items := make([]orderItem, d.oOlCnt)

	itemIDs := make(map[int]struct{}, d.oOlCnt)

	for i := 0; i < len(items); i++ {
		items[i].olNumber = i + 1
		if i == len(items)-1 && rbk == 1 {
			items[i].olIID = -1
		} else {
			for {
				id := randItemID(s.R)
				// Find a unique ID
				if _, ok := itemIDs[id]; ok {
					continue
				}
				itemIDs[id] = struct{}{}
				items[i].olIID = id
				break
			}
		}

		if w.cfg.Warehouses == 1 || randInt(s.R, 1, 100) != 1 {
			items[i].olSupplyWID = d.wID
		} else {
			items[i].olSupplyWID = w.otherWarehouse(ctx, d.wID)
			allLocal = 0
		}

		items[i].olQuantity = randInt(s.R, 1, 10)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].olIID < items[j].olIID
	})

	tx, err := s.Conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.IsolationLevel(w.cfg.Isolation),
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// TODO: support prepare statement

	// Process 1
	// SELECT c_discount, c_last, c_credit, w_tax INTO :c_discount, :c_last, :c_credit,
	// :w_tax FROM customer, warehouse WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
	query := `SELECT c_discount, c_last, c_credit, w_tax FROM customer, 
warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?`

	if err := tx.QueryRowContext(ctx, query, d.wID, d.dID, d.cID).Scan(&d.cDiscount, &d.cLast, &d.cCredit, &d.wTax); err != nil {
		return err
	}

	// Process 2
	// SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax FROM district WHERE d_id = :d_id AND d_w_id = :w_id FOR UPDATE;

	query = `SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ? FOR UPDATE`
	if err := tx.QueryRowContext(ctx, query, d.dID, d.wID).Scan(&d.dNextOID, &d.dTax); err != nil {
		return err
	}

	// Process 3

	// UPDATE district SET d_next_o_id = :d_next_o_id + 1 WHERE d_id = :d_id AND d_w_id = :w_id;
	query = "UPDATE district SET d_next_o_id = ? + 1 WHERE d_id = ? AND d_w_id = ?"
	if _, err := tx.ExecContext(ctx, query, d.dNextOID, d.dID, d.wID); err != nil {
		return err
	}

	oID := d.dNextOID

	// Process 4

	// INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
	// 	VALUES (:o_id , :d _id , :w _id , :c_id , :datetime, :o_ol_cnt, :o_all_local);
	query = `INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) 
VALUES (?, ?, ?, ?, ?, ?, ?)`
	if _, err := tx.ExecContext(ctx, query, oID, d.dID, d.wID, d.cID,
		time.Now().Format(timeFormat), d.oOlCnt, allLocal); err != nil {
		return err
	}

	// Process 5

	// INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (:o_id , :d _id , :w _id );
	query = `INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)`
	if _, err := tx.ExecContext(ctx, query, oID, d.dID, d.wID); err != nil {
		return err
	}

	for i := 0; i < d.oOlCnt; i++ {
		item := items[i]
		// Process 6

		// SELECT i_price, i_name , i_data INTO :i_price, :i_name, :i_data FROM item WHERE i_id = :ol_i_id;
		query = "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?"

		if err := tx.QueryRowContext(ctx, query, item.olIID).Scan(&item.iPrice, &item.iName, &item.iData); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}

		// Process 7

		// SELECT s_quantity, s_data, s_dist_01, s_dist_02,
		// 	s_dist_03, s_dist_04, s_dist_05, s_dist_06,
		// 	s_dist_07, s_dist_08, s_dist_09, s_dist_10
		// INTO :s_quantity, :s_data, :s_dist_01, :s_dist_02,
		// 	:s_dist_03, :s_dist_04, :s_dist_05, :s_dist_06,
		// 	:s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
		// FROM stock WHERE s_i_id = :ol_i_id
		// 	AND s_w_id = :ol_supply_w_id FOR UPDATE;
		query = fmt.Sprintf(`SELECT s_quantity, s_data, s_dist_%02d s_dist FROM stock 
WHERE s_i_id = ? AND s_w_id = ? FOR UPDATE`, d.dID)

		var distInfo struct {
			sQuantity int    `db:"s_quantity"`
			sData     string `db:"s_data"`
			sDist     string `db:"s_dist"`
		}
		if err := tx.QueryRowContext(ctx, query, item.olIID, item.olSupplyWID).Scan(&distInfo.sQuantity, &distInfo.sData, &distInfo.sDist); err != nil {
			return err
		}

		if distInfo.sQuantity > item.olQuantity {
			distInfo.sQuantity = distInfo.sQuantity - item.olQuantity
		} else {
			distInfo.sQuantity = distInfo.sQuantity - item.olQuantity + 91
		}

		// Process 8

		// UPDATE stock SET s_quantity = :s_quantity
		//  WHERE s_i_id = :ol_i_id
		// 	AND s_w_id = :ol_supply_w_id;
		query = "UPDATE stock SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?"
		if _, err := tx.ExecContext(ctx, query, distInfo.sQuantity, item.olIID, item.olSupplyWID); err != nil {
			return err
		}

		olAmount := float64(item.olQuantity) * item.iPrice * (1 + d.wTax + d.dTax) * (1 - d.cDiscount)

		// Process 9

		// INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id,
		// 	ol_number, ol_i_id,
		// 	ol_supply_w_id, ol_quantity,
		// 	ol_amount, ol_dist_info)
		// VALUES (:o_id, :d_id, :w_id, :ol_number, :ol_i_id,
		//  :ol_supply_w_id, :ol_quantity, :ol_amount, :ol_dist_info);
		query = `INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id,
ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
		if _, err := tx.ExecContext(ctx, query, oID, d.dID, d.wID, item.olNumber,
			item.olIID, item.olSupplyWID, item.olQuantity, olAmount, distInfo.sDist); err != nil {
			return err
		}
	}

	return tx.Commit()
}
