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
