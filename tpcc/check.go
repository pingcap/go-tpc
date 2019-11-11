package tpcc

import (
	"context"
	"fmt"
)

// Check implements Workloader interface
func (w *Workloader) Check(ctx context.Context, threadID int) error {
	// refer 3.3.2
	checks := []func(ctx context.Context, warehouse int) error{
		w.checkCondition1,
		w.checkCondition2,
	}

	for i := threadID % w.cfg.Threads; i < w.cfg.Warehouses; i += w.cfg.Threads {
		warehouse := i%w.cfg.Warehouses + 1
		for i := 0; i < len(checks); i++ {
			if err := checks[i](ctx, warehouse); err != nil {
				return fmt.Errorf("check condition %d failed %v", i+1, err)
			}
		}
	}

	return nil
}

func (w *Workloader) checkCondition1(ctx context.Context, warehouse int) error {
	s := w.getState(ctx)

	// Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship:
	// 	W_YTD = sum(D_YTD)
	var diff float64
	query := "SELECT sum(d_ytd) - max(w_ytd) diff FROM district, warehouse WHERE d_w_id = w_id AND w_id = ? group by d_w_id"

	rows, err := s.Conn.QueryContext(ctx, query, warehouse)
	if err != nil {
		return fmt.Errorf("Exec %s failed %v", query, err)
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&diff); err != nil {
			return err
		}

		if diff != 0 {
			return fmt.Errorf("sum(d_ytd) - max(w_ytd) should be 0 in warehouse %d, but got %f", warehouse, diff)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (w *Workloader) checkCondition2(ctx context.Context, warehouse int) error {
	s := w.getState(ctx)

	// Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
	// D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
	// for each district defined by (D_W_ID = O_W_ID = NO_W_ID) and (D_ID = O_D_ID = NO_D_ID). This condition
	// does not apply to the NEW-ORDER table for any districts which have no outstanding new orders (i.e., the numbe r of
	// rows is zero).

	var diff float64
	query := "SELECT POWER((o_nexi_o_id -1 - max(o_id)), 2) + POWER((o_nexi_o_id -1 - max(no_o_id)), 2) FROM district, order, new_order, warehouse WHERE d_w_id = o_w_id AND d_w_id = no_w_id AND d_id = o_d_id AND d_id = no_d_id group by d_w_id"

	rows, err := s.Conn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("Exec %s failed %v", query, err)
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&diff); err != nil {
			return err
		}

		if diff != 0 {
			return fmt.Errorf("POWER((o_nexi_o_id -1 - max(o_id)), 2) + POWER((o_nexi_o_id -1 - max(no_o_id)), 2) != 0 , but got %f", diff)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}