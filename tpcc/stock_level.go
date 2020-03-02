package tpcc

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const stockLevelCount = `SELECT COUNT(DISTINCT (s_i_id)) stock_count FROM order_line, stock 
WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= ? - 20 AND s_w_id = ? AND s_i_id = ol_i_id AND s_quantity < ?`
const stockLevelSelectDistrict = `SELECT d_next_o_id FROM district WHERE d_pk = ?`
const stockLevelSelectRecentOrderIDs = `SELECT o_id FROM orders WHERE o_pk BETWEEN ? AND ?`

func (w *Workloader) runStockLevel(ctx context.Context, thread int) error {
	s := w.getState(ctx)

	tx, err := w.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	wID := randInt(s.R, 1, w.cfg.Warehouses)
	dID := randInt(s.R, 1, 10)
	threshold := randInt(s.R, 10, 20)

	// SELECT d_next_o_id INTO :o_id FROM district WHERE d_w_id=:w_id AND d_id=:d_id;

	var nextOID int
	if err := s.stockLevelStmt[stockLevelSelectDistrict].QueryRowContext(ctx, getDPK(wID, dID)).Scan(&nextOID); err != nil {
		return err
	}
	var rows *sql.Rows
	if rows, err = s.stockLevelStmt[stockLevelSelectRecentOrderIDs].QueryContext(ctx, getOPK(wID, dID, nextOID-20), getOPK(wID, dID, nextOID-1)); err != nil {
		return err
	}
	oIDs := make([]int, 0, 20)
	for rows.Next() {
		var oID int
		if err = rows.Scan(&oID); err != nil {
			return err
		}
		oIDs = append(oIDs, oID)
	}
	var betweenConditions []string
	for _, oID := range oIDs {
		betweenConditions = append(betweenConditions, fmt.Sprintf("ol_pk BETWEEN %d AND %d", getOLPK(wID, dID, oID, 0), getOLPK(wID, dID, oID+1, 0)))
	}
	strings.Join(betweenConditions, " OR ")
	selectRecentItemsSQL := `SELECT DISTINCT(ol_i_id) FROM order_line WHERE ` + strings.Join(betweenConditions, " OR ")
	if rows, err = tx.QueryContext(ctx, selectRecentItemsSQL); err != nil {
		return err
	}
	var iIDs []int
	for rows.Next() {
		var iID int
		if err = rows.Scan(&iID); err != nil {
			return err
		}
		iIDs = append(iIDs, iID)
	}
	sort.Ints(iIDs)

	var stockPKs []string
	for _, iID := range iIDs {
		stockPKs = append(stockPKs, strconv.Itoa(iID))
	}
	var stockCount int
	selectStockLevelSQL := fmt.Sprintf(`SELECT COUNT(*) stock_count FROM stock WHERE s_pk IN (%s) AND s_quantity < %d`, strings.Join(stockPKs, ","), threshold)
	if err = tx.QueryRowContext(ctx, selectStockLevelSQL).Scan(&stockCount); err != nil {
		return err
	}
	return tx.Commit()
}
