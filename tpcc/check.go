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
		w.checkCondition3,
		w.checkCondition4,
		w.checkCondition5,
		w.checkCondition6,
		w.checkCondition7,
		w.checkCondition8,
		w.checkCondition9,
		w.checkCondition10,
		w.checkCondition12,
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
	query := "SELECT POWER((d_next_o_id -1 - mo), 2) + POWER((d_next_o_id -1 - mno), 2) diff FROM district dis, (SELECT o_d_id,max(o_id) mo FROM orders WHERE o_w_id= ? GROUP BY o_d_id) q, (select no_d_id,max(no_o_id) mno from new_order where no_w_id= ? group by no_d_id) no where d_w_id = ? and q.o_d_id=dis.d_id and no.no_d_id=dis.d_id"

	rows, err := s.Conn.QueryContext(ctx, query, warehouse, warehouse, warehouse)
	if err != nil {
		return fmt.Errorf("Exec %s failed %v", query, err)
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&diff); err != nil {
			return err
		}

		if diff != 0 {
			return fmt.Errorf("POWER((d_next_o_id -1 - mo), 2) + POWER((d_next_o_id -1 - mno),2) != 0 in warehouse %d, but got %f", warehouse, diff)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (w *Workloader) checkCondition3(ctx context.Context, warehouse int) error {
	s := w.getState(ctx)

	var diff float64

	query := "SELECT max(no_o_id)-min(no_o_id)+1 - count(*) diff from new_order where no_w_id = ? group by no_d_id"

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
			return fmt.Errorf("max(no_o_id)-min(no_o_id)+1 - count(*) in warehouse %d, but got %f", warehouse, diff)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (w *Workloader) checkCondition4(ctx context.Context, warehouse int) error {
	s := w.getState(ctx)

	var diff float64

	query := "SELECT count(*) FROM (SELECT o_d_id, SUM(o_ol_cnt) sm1, MAX(cn) as cn FROM orders,(SELECT ol_d_id, COUNT(*) cn FROM order_line WHERE ol_w_id = ? GROUP BY ol_d_id) ol WHERE o_w_id = ? AND ol_d_id=o_d_id GROUP BY o_d_id) t1 WHERE sm1<>cn"

	rows, err := s.Conn.QueryContext(ctx, query, warehouse, warehouse)
	if err != nil {
		return fmt.Errorf("Exec %s failed %v", query, err)
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&diff); err != nil {
			return err
		}

		if diff != 0 {
			return fmt.Errorf("count(*) in warehouse %d, but got %f", warehouse, diff)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (w *Workloader) checkCondition5(ctx context.Context, warehouse int) error {
	s := w.getState(ctx)

	var diff float64

	query := "SELECT count(*)  FROM orders LEFT JOIN new_order ON (no_w_id=o_w_id AND o_d_id=no_d_id AND o_id=no_o_id) where o_w_id = ? and ((o_carrier_id IS NULL and no_o_id IS  NULL) OR (o_carrier_id IS NOT NULL and no_o_id IS NOT NULL  )) "

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
			return fmt.Errorf("count(*) in warehouse %d, but got %f", warehouse, diff)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

// TODO: Implement this
func (w *Workloader) checkCondition6(ctx context.Context, warehouse int) error {
	return nil
}

func (w *Workloader) checkCondition7(ctx context.Context, warehouse int) error {
	s := w.getState(ctx)

	var diff float64

	query := "SELECT count(*) FROM orders, order_line WHERE o_id=ol_o_id AND o_d_id=ol_d_id AND ol_w_id=o_w_id AND o_w_id = ? AND ((ol_delivery_d IS NULL and o_carrier_id IS NOT NULL) or (o_carrier_id IS NULL and ol_delivery_d IS NOT NULL ))"

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
			return fmt.Errorf("count(*) in warehouse %d, but got %f", warehouse, diff)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (w *Workloader) checkCondition8(ctx context.Context, warehouse int) error {
	s := w.getState(ctx)

	var diff float64

	query := "SELECT count(*) cn FROM (SELECT w_id,w_ytd,SUM(h_amount) sm FROM history,warehouse WHERE h_w_id=w_id and w_id = ? GROUP BY w_id) t1 WHERE w_ytd<>sm"

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
			return fmt.Errorf("count(*) in warehouse %d, but got %f", warehouse, diff)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (w *Workloader) checkCondition9(ctx context.Context, warehouse int) error {
	s := w.getState(ctx)

	var diff float64

	query := "SELECT COUNT(*) FROM (select d_id,d_w_id,sum(d_ytd) s1 from district group by d_id,d_w_id) d,(select h_d_id,h_w_id,sum(h_amount) s2 from history WHERE  h_w_id = ? group by h_d_id, h_w_id) h WHERE h_d_id=d_id AND d_w_id=h_w_id and d_w_id= ? and s1<>s2"

	rows, err := s.Conn.QueryContext(ctx, query, warehouse, warehouse)
	if err != nil {
		return fmt.Errorf("Exec %s failed %v", query, err)
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&diff); err != nil {
			return err
		}

		if diff != 0 {
			return fmt.Errorf("count(*) in warehouse %d, but got %f", warehouse, diff)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (w *Workloader) checkCondition10(ctx context.Context, warehouse int) error {
	s := w.getState(ctx)

	var diff float64

	query := `SELECT count(*) 
	FROM (  SELECT  c.c_id, c.c_d_id, c.c_w_id, c.c_balance c1, 
				   (SELECT sum(ol_amount) FROM orders STRAIGHT_JOIN order_line 
					 WHERE OL_W_ID=O_W_ID 
					   AND OL_D_ID = O_D_ID 
					   AND OL_O_ID = O_ID 
					   AND OL_DELIVERY_D IS NOT NULL 
					   AND O_W_ID=c.c_w_id 
					   AND O_D_ID=c.C_D_ID 
					   AND O_C_ID=c.C_ID) sm, (SELECT  sum(h_amount)  from  history 
												WHERE H_C_W_ID=c.C_W_ID 
												  AND H_C_D_ID=c.C_D_ID 
												  AND H_C_ID=c.C_ID) smh 
			 FROM customer c 
			WHERE  c.c_w_id = ? ) t
   WHERE c1<>sm-smh`

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
			return fmt.Errorf("count(*) in warehouse %d, but got %f", warehouse, diff)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

// TODO: Implement this
func (w *Workloader) checkCondition11(ctx context.Context, warehouse int) error {
	return nil
}

func (w *Workloader) checkCondition12(ctx context.Context, warehouse int) error {
	s := w.getState(ctx)

	var diff float64

	query := `SELECT count(*) FROM (SELECT  c.c_id, c.c_d_id, c.c_balance c1, c_ytd_payment, 
		(SELECT sum(ol_amount) FROM orders STRAIGHT_JOIN order_line 
		WHERE OL_W_ID=O_W_ID AND OL_D_ID = O_D_ID AND OL_O_ID = O_ID AND OL_DELIVERY_D IS NOT NULL AND 
		O_W_ID=c.c_w_id AND O_D_ID=c.C_D_ID AND O_C_ID=c.C_ID) sm FROM customer c WHERE  c.c_w_id = ?) t1 
		WHERE c1+c_ytd_payment <> sm`
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
			return fmt.Errorf("count(*) in warehouse %d, but got %f", warehouse, diff)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}
