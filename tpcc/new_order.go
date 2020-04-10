package tpcc

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"
)

const (
	newOrderSelectCustomer = `SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_pk = ?`
	newOrderSelectDistrict = `SELECT d_next_o_id, d_tax FROM district WHERE d_pk = ? FOR UPDATE`
	newOrderUpdateDistrict = `UPDATE district SET d_next_o_id = ? + 1 WHERE d_pk = ?`
	newOrderInsertOrder    = `INSERT INTO orders (o_pk, o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	newOrderInsertNewOrder = `INSERT INTO new_order (no_pk, no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?, ?)`
	newOrderUpdateStock    = `UPDATE stock SET s_quantity = ?, s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + ? WHERE s_pk = ?`
)

var (
	newOrderSelectItemSQLs      [16]string
	newOrderSelectStockSQLs     [16]string
	newOrderInsertOrderLineSQLs [16]string
)

func init() {
	for i := 5; i <= 15; i++ {
		newOrderSelectItemSQLs[i] = genNewOrderSelectItemsSQL(i)
		newOrderSelectStockSQLs[i] = genNewOrderSelectStockSQL(i)
		newOrderInsertOrderLineSQLs[i] = genNewOrderInsertOrderLineSQL(i)
	}
}

func genNewOrderSelectItemsSQL(cnt int) string {
	buf := bytes.NewBufferString("SELECT i_price, i_name, i_data, i_id FROM item WHERE i_id IN (")
	for i := 0; i < cnt; i++ {
		if i != 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('?')
	}
	buf.WriteByte(')')
	return buf.String()
}

func genNewOrderSelectStockSQL(cnt int) string {
	buf := bytes.NewBufferString("SELECT s_i_id, s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_pk IN (")
	for i := 0; i < cnt; i++ {
		if i != 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('?')
	}
	buf.WriteString(") FOR UPDATE")
	return buf.String()
}

func genNewOrderInsertOrderLineSQL(cnt int) string {
	buf := bytes.NewBufferString("INSERT into order_line (ol_pk, ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES ")
	for i := 0; i < cnt; i++ {
		if i != 0 {
			buf.WriteByte(',')
		}
		buf.WriteString("(?,?,?,?,?,?,?,?,?,?)")
	}
	return buf.String()
}

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
	olDeliveryD sql.NullString

	iPrice float64
	iName  string
	iData  string

	foundInItems    bool
	foundInStock    bool
	sQuantity       int
	sDist           string
	remoteWarehouse int
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
		dID:    randInt(s.R, 1, districtPerWarehouse),
		cID:    randCustomerID(s.R),
		oOlCnt: randInt(s.R, 5, 15),
	}

	rbk := randInt(s.R, 1, 100)
	allLocal := 1

	items := make([]orderItem, d.oOlCnt)

	itemsMap := make(map[int]*orderItem, d.oOlCnt)

	for i := 0; i < len(items); i++ {
		item := &items[i]
		item.olNumber = i + 1
		if i == len(items)-1 && rbk == 1 {
			item.olIID = -1
		} else {
			for {
				id := randItemID(s.R)
				// Find a unique ID
				if _, ok := itemsMap[id]; ok {
					continue
				}
				itemsMap[id] = item
				item.olIID = id
				break
			}
		}

		if w.cfg.Warehouses == 1 || randInt(s.R, 1, 100) != 1 {
			item.olSupplyWID = d.wID
		} else {
			item.olSupplyWID = w.otherWarehouse(ctx, d.wID)
			item.remoteWarehouse = 1
			allLocal = 0
		}

		item.olQuantity = randInt(s.R, 1, 10)
	}

	tx, err := w.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// TODO: support prepare statement

	// Process 1
	if err := s.newOrderStmts[newOrderSelectCustomer].QueryRowContext(ctx, d.wID, getCPK(d.wID, d.dID, d.cID)).Scan(&d.cDiscount, &d.cLast, &d.cCredit, &d.wTax); err != nil {
		return fmt.Errorf("exec %s failed %v", newOrderSelectCustomer, err)
	}

	// Process 2
	if err := s.newOrderStmts[newOrderSelectDistrict].QueryRowContext(ctx, getDPK(d.wID, d.dID)).Scan(&d.dNextOID, &d.dTax); err != nil {
		return fmt.Errorf("exec %s failed %v", newOrderSelectDistrict, err)
	}

	// Process 3
	if _, err := s.newOrderStmts[newOrderUpdateDistrict].ExecContext(ctx, d.dNextOID, getDPK(d.wID, d.dID)); err != nil {
		return fmt.Errorf("exec %s failed %v", newOrderUpdateDistrict, err)
	}

	oID := d.dNextOID

	// Process 4
	if _, err := s.newOrderStmts[newOrderInsertOrder].ExecContext(ctx, getOPK(d.wID, d.dID, oID), oID, d.dID, d.wID, d.cID,
		time.Now().Format(timeFormat), d.oOlCnt, allLocal); err != nil {
		return fmt.Errorf("exec %s failed %v", newOrderInsertOrder, err)
	}

	// Process 5

	// INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (:o_id , :d _id , :w _id );
	// query = `INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)`
	if _, err := s.newOrderStmts[newOrderInsertNewOrder].ExecContext(ctx, getNOPK(d.wID, d.dID, oID), oID, d.dID, d.wID); err != nil {
		return fmt.Errorf("exec %s failed %v", newOrderInsertNewOrder, err)
	}

	// Process 6
	selectItemSQL := newOrderSelectItemSQLs[len(items)]
	selectItemArgs := make([]interface{}, len(items))
	for i := range items {
		selectItemArgs[i] = items[i].olIID
	}
	rows, err := s.newOrderStmts[selectItemSQL].QueryContext(ctx, selectItemArgs...)
	if err != nil {
		return fmt.Errorf("exec %s failed %v", selectItemSQL, err)
	}
	for rows.Next() {
		var tmpItem orderItem
		err := rows.Scan(&tmpItem.iPrice, &tmpItem.iName, &tmpItem.iData, &tmpItem.olIID)
		if err != nil {
			return fmt.Errorf("exec %s failed %v", selectItemSQL, err)
		}
		item := itemsMap[tmpItem.olIID]
		item.iPrice = tmpItem.iPrice
		item.iName = tmpItem.iName
		item.iData = tmpItem.iData
		item.foundInItems = true
	}
	for i := range items {
		item := &items[i]
		if !item.foundInItems {
			if item.olIID == -1 {
				// Rollback
				return nil
			}
			return fmt.Errorf("item %d not found", item.olIID)
		}
	}

	// Process 7
	selectStockSQL := newOrderSelectStockSQLs[len(items)]
	selectStockArgs := make([]interface{}, len(items))
	for i := range items {
		selectStockArgs[i] = getSPK(d.wID, items[i].olIID)
	}
	rows, err = s.newOrderStmts[selectStockSQL].QueryContext(ctx, selectStockArgs...)
	if err != nil {
		return fmt.Errorf("exec %s failed %v", selectStockSQL, err)
	}
	for rows.Next() {
		var iID int
		var quantity int
		var data string
		var dists [10]string
		err = rows.Scan(&iID, &quantity, &data, &dists[0], &dists[1], &dists[2], &dists[3], &dists[4], &dists[5], &dists[6], &dists[7], &dists[8], &dists[9])
		if err != nil {
			return fmt.Errorf("exec %s failed %v", selectStockSQL, err)
		}
		item := itemsMap[iID]
		quantity -= item.olQuantity
		if quantity < 10 {
			quantity += 91
		}
		item.foundInStock = true
		item.sQuantity = quantity
		item.sDist = dists[d.dID-1]
		item.olAmount = float64(item.olQuantity) * item.iPrice * (1 + d.wTax + d.dTax) * (1 - d.cDiscount)
	}

	// Process 8
	for i := 0; i < d.oOlCnt; i++ {
		item := &items[i]
		if !item.foundInStock {
			return fmt.Errorf("item (%d, %d) not found in stock", d.wID, item.olIID)
		}
		if item.olIID < 0 {
			return nil
		}
		if _, err = s.newOrderStmts[newOrderUpdateStock].ExecContext(ctx, item.sQuantity, item.olQuantity, item.remoteWarehouse, getSPK(d.wID, item.olIID)); err != nil {
			return fmt.Errorf("exec %s failed %v", newOrderUpdateStock, err)
		}
	}

	// Process 9
	insertOrderLineSQL := newOrderInsertOrderLineSQLs[len(items)]
	insertOrderLineArgs := make([]interface{}, len(items)*10)
	for i := range items {
		item := &items[i]
		insertOrderLineArgs[i*10] = getOLPK(d.wID, d.dID, oID, item.olNumber)
		insertOrderLineArgs[i*10+1] = oID
		insertOrderLineArgs[i*10+2] = d.dID
		insertOrderLineArgs[i*10+3] = d.wID
		insertOrderLineArgs[i*10+4] = item.olNumber
		insertOrderLineArgs[i*10+5] = item.olIID
		insertOrderLineArgs[i*10+6] = item.olSupplyWID
		insertOrderLineArgs[i*10+7] = item.olQuantity
		insertOrderLineArgs[i*10+8] = item.olAmount
		insertOrderLineArgs[i*10+9] = item.sDist
	}
	if _, err = s.newOrderStmts[insertOrderLineSQL].ExecContext(ctx, insertOrderLineArgs...); err != nil {
		return fmt.Errorf("exec %s failed %v", insertOrderLineSQL, err)
	}
	return tx.Commit()
}
