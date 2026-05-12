package tpcc

import (
	"context"
	"database/sql"
	"fmt"
)

// procs_pg.go — PostgreSQL stored-procedure mode for TPC-C.
//
// When --stored-procs is set (postgres driver only), each TPC-C transaction is
// implemented as a PL/pgSQL PROCEDURE installed via CREATE OR REPLACE at the
// start of Run(). Workers then invoke them with `CALL tpcc_xxx(...)` instead of
// running the multi-statement transaction client-side. That eliminates ~10
// round-trips per NEW_ORDER and is what HammerDB / DBT-2 call "stored
// procedure mode".
//
// Procedures are installed declaratively (CREATE OR REPLACE) — no Prepare
// step is required and the same dataset can be benchmarked in either mode.
//
// PROCEDURE (not FUNCTION) is used everywhere so NEW_ORDER's 1%-rollback
// case can issue an in-band ROLLBACK, matching the original Go behaviour
// where district/orders/new_order writes are done and then undone.

// ---------------------------------------------------------------------------
// CALL strings used by the per-transaction stored-proc wrappers
// ---------------------------------------------------------------------------

const (
	tpccCallNewOrder    = `CALL tpcc_new_order($1,$2,$3,$4,$5,$6,$7,$8,$9)`
	tpccCallPayment     = `CALL tpcc_payment($1,$2,$3,$4,$5,$6,$7,$8)`
	tpccCallOrderStatus = `CALL tpcc_order_status($1,$2,$3,$4)`
	tpccCallDelivery    = `CALL tpcc_delivery($1,$2,$3)`
	tpccCallStockLevel  = `CALL tpcc_stock_level($1,$2,$3)`
)

// ---------------------------------------------------------------------------
// PL/pgSQL bodies
// ---------------------------------------------------------------------------

// tpcc_new_order(p_w_id, p_d_id, p_c_id, p_ol_cnt,
//                p_ol_supply_w_id INT[], p_ol_i_id INT[], p_ol_quantity INT[],
//                p_entry_d TIMESTAMP, p_all_local INT)
//
// 1% of calls intentionally pass an item id of -1 in the last position to
// drive the spec-mandated rollback path; the procedure performs the
// district/orders/new_order writes first (so the engine pays for them),
// then issues ROLLBACK and returns normally — caller sees success.
const procNewOrder = `
CREATE OR REPLACE PROCEDURE tpcc_new_order(
    p_w_id           INT,
    p_d_id           INT,
    p_c_id           INT,
    p_ol_cnt         INT,
    p_ol_supply_w_id INT[],
    p_ol_i_id        INT[],
    p_ol_quantity    INT[],
    p_entry_d        TIMESTAMP,
    p_all_local      INT
) LANGUAGE plpgsql AS $$
DECLARE
    v_c_discount   NUMERIC;
    v_w_tax        NUMERIC;
    v_d_tax        NUMERIC;
    v_d_next_o_id  INT;
    v_o_id         INT;

    v_prices   NUMERIC[] := ARRAY[]::NUMERIC[];
    v_rollback BOOLEAN   := FALSE;

    v_s_quantity INT;
    v_s_data     VARCHAR;
    v_s_dist     CHAR(24);
    v_remote_cnt INT;
    v_amount     NUMERIC;
    v_i_price    NUMERIC;
    v_i_name     VARCHAR;
    v_i_data     VARCHAR;
    i INT;
BEGIN
    SELECT c_discount, w_tax
      INTO v_c_discount, v_w_tax
      FROM customer, warehouse
     WHERE w_id = p_w_id
       AND c_w_id = w_id
       AND c_d_id = p_d_id
       AND c_id   = p_c_id;

    SELECT d_next_o_id, d_tax
      INTO v_d_next_o_id, v_d_tax
      FROM district
     WHERE d_id = p_d_id AND d_w_id = p_w_id
       FOR UPDATE;

    UPDATE district
       SET d_next_o_id = v_d_next_o_id + 1
     WHERE d_id = p_d_id AND d_w_id = p_w_id;

    v_o_id := v_d_next_o_id;

    INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
    VALUES (v_o_id, p_d_id, p_w_id, p_c_id, p_entry_d, p_ol_cnt, p_all_local);

    INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
    VALUES (v_o_id, p_d_id, p_w_id);

    -- First pass over items: lookup prices, detect intentional rollback.
    -- An i_id of -1 marks the 1%-rollback case; any other unknown id is a
    -- real error and aborts the procedure with an exception.
    FOR i IN 1..p_ol_cnt LOOP
        IF p_ol_i_id[i] = -1 THEN
            v_rollback := TRUE;
            v_prices := array_append(v_prices, 0::NUMERIC);
            CONTINUE;
        END IF;

        SELECT i_price, i_name, i_data
          INTO v_i_price, v_i_name, v_i_data
          FROM item
         WHERE i_id = p_ol_i_id[i];
        IF NOT FOUND THEN
            RAISE EXCEPTION 'item % not found', p_ol_i_id[i];
        END IF;
        v_prices := array_append(v_prices, v_i_price);
    END LOOP;

    IF v_rollback THEN
        ROLLBACK;
        RETURN;
    END IF;

    -- Second pass: stock lookup, stock update, order_line insert.
    FOR i IN 1..p_ol_cnt LOOP
        -- District-specific stock distribution column (s_dist_01 .. s_dist_10).
        EXECUTE format(
            'SELECT s_quantity, s_data, s_dist_%s FROM stock '
            'WHERE s_w_id = $1 AND s_i_id = $2 FOR UPDATE',
            to_char(p_d_id, 'FM00')
        )
        INTO v_s_quantity, v_s_data, v_s_dist
        USING p_w_id, p_ol_i_id[i];

        v_s_quantity := v_s_quantity - p_ol_quantity[i];
        IF v_s_quantity < 10 THEN
            v_s_quantity := v_s_quantity + 91;
        END IF;

        IF p_ol_supply_w_id[i] <> p_w_id THEN
            v_remote_cnt := 1;
        ELSE
            v_remote_cnt := 0;
        END IF;

        UPDATE stock
           SET s_quantity   = v_s_quantity,
               s_ytd        = s_ytd + p_ol_quantity[i],
               s_order_cnt  = s_order_cnt + 1,
               s_remote_cnt = s_remote_cnt + v_remote_cnt
         WHERE s_i_id = p_ol_i_id[i] AND s_w_id = p_w_id;

        v_amount := p_ol_quantity[i]::NUMERIC * v_prices[i]
                  * (1 + v_w_tax + v_d_tax) * (1 - v_c_discount);

        INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id,
                                ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
        VALUES (v_o_id, p_d_id, p_w_id, i, p_ol_i_id[i],
                p_ol_supply_w_id[i], p_ol_quantity[i], v_amount, v_s_dist);
    END LOOP;
END;
$$;
`

// tpcc_payment(p_w_id, p_d_id, p_c_w_id, p_c_d_id, p_h_amount,
//              p_c_id INT (NULL → search by c_last), p_c_last VARCHAR (or NULL),
//              p_h_date TIMESTAMP)
const procPayment = `
CREATE OR REPLACE PROCEDURE tpcc_payment(
    p_w_id       INT,
    p_d_id       INT,
    p_c_w_id     INT,
    p_c_d_id     INT,
    p_h_amount   NUMERIC,
    p_c_id       INT,
    p_c_last     VARCHAR,
    p_h_date     TIMESTAMP
) LANGUAGE plpgsql AS $$
DECLARE
    v_w_name      VARCHAR;
    v_d_name      VARCHAR;
    v_c_id        INT;
    v_c_first     VARCHAR;
    v_c_middle    VARCHAR;
    v_c_last      VARCHAR;
    v_c_credit    CHAR(2);
    v_c_balance   NUMERIC;
    v_c_data      VARCHAR;
    v_new_data    VARCHAR;
    v_h_data      VARCHAR;
    v_namecnt     INT;
    v_match_pos   INT;
BEGIN
    UPDATE district SET d_ytd = d_ytd + p_h_amount
     WHERE d_w_id = p_w_id AND d_id = p_d_id;
    SELECT d_name INTO v_d_name FROM district
     WHERE d_w_id = p_w_id AND d_id = p_d_id;

    UPDATE warehouse SET w_ytd = w_ytd + p_h_amount
     WHERE w_id = p_w_id;
    SELECT w_name INTO v_w_name FROM warehouse
     WHERE w_id = p_w_id;

    IF p_c_id IS NULL THEN
        SELECT COUNT(c_id) INTO v_namecnt FROM customer
         WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_last = p_c_last;
        IF v_namecnt = 0 THEN
            RAISE EXCEPTION 'customer not found for last=%', p_c_last;
        END IF;
        IF v_namecnt % 2 = 1 THEN
            v_namecnt := v_namecnt + 1;
        END IF;
        v_match_pos := v_namecnt / 2;

        SELECT c_id INTO v_c_id FROM (
            SELECT c_id, ROW_NUMBER() OVER (ORDER BY c_first) AS rn
              FROM customer
             WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_last = p_c_last
        ) sub WHERE sub.rn = v_match_pos;
    ELSE
        v_c_id := p_c_id;
    END IF;

    SELECT c_first, c_middle, c_last, c_credit, c_balance
      INTO v_c_first, v_c_middle, v_c_last, v_c_credit, v_c_balance
      FROM customer
     WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_c_id
       FOR UPDATE;

    IF v_c_credit = 'BC' THEN
        SELECT c_data INTO v_c_data FROM customer
         WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_c_id;

        v_new_data := format('| %s %s %s %s %s $%s %s %s',
                              v_c_id, p_c_d_id, p_c_w_id, p_d_id, p_w_id,
                              p_h_amount,
                              to_char(p_h_date, 'YYYY-MM-DD HH24:MI:SS'),
                              v_c_data);
        IF length(v_new_data) > 500 THEN
            v_new_data := substr(v_new_data, 1, 500);
        END IF;

        UPDATE customer
           SET c_balance     = c_balance - p_h_amount,
               c_ytd_payment = c_ytd_payment + p_h_amount,
               c_payment_cnt = c_payment_cnt + 1,
               c_data        = v_new_data
         WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_c_id;
    ELSE
        UPDATE customer
           SET c_balance     = c_balance - p_h_amount,
               c_ytd_payment = c_ytd_payment + p_h_amount,
               c_payment_cnt = c_payment_cnt + 1
         WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = v_c_id;
    END IF;

    v_h_data := substr(v_w_name || '    ' || v_d_name, 1, 24);
    INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id,
                         h_date, h_amount, h_data)
    VALUES (p_c_d_id, p_c_w_id, v_c_id, p_d_id, p_w_id,
            p_h_date, p_h_amount, v_h_data);
END;
$$;
`

// tpcc_order_status(p_w_id, p_d_id, p_c_id INT (NULL → by-name), p_c_last VARCHAR)
//
// Read-only. Mirrors the Go side: count by last name, pick the (cnt/2)th
// by c_first; or look up by c_id directly. Then latest order and its
// order_line rows are read (results discarded — only the engine work matters).
const procOrderStatus = `
CREATE OR REPLACE PROCEDURE tpcc_order_status(
    p_w_id   INT,
    p_d_id   INT,
    p_c_id   INT,
    p_c_last VARCHAR
) LANGUAGE plpgsql AS $$
DECLARE
    v_c_id      INT;
    v_c_balance NUMERIC;
    v_c_first   VARCHAR;
    v_c_middle  VARCHAR;
    v_c_last    VARCHAR;
    v_o_id      INT;
    v_o_entry_d TIMESTAMP;
    v_o_carrier_id INT;
    v_namecnt   INT;
    v_match_pos INT;
    r RECORD;
BEGIN
    IF p_c_id IS NULL THEN
        SELECT COUNT(c_id) INTO v_namecnt FROM customer
         WHERE c_w_id = p_w_id AND c_d_id = p_d_id AND c_last = p_c_last;
        IF v_namecnt = 0 THEN
            RAISE EXCEPTION 'customer not found for last=%', p_c_last;
        END IF;
        IF v_namecnt % 2 = 1 THEN
            v_namecnt := v_namecnt + 1;
        END IF;
        v_match_pos := v_namecnt / 2;

        SELECT sub.c_balance, sub.c_first, sub.c_middle, sub.c_id
          INTO v_c_balance, v_c_first, v_c_middle, v_c_id
          FROM (
            SELECT c_balance, c_first, c_middle, c_id,
                   ROW_NUMBER() OVER (ORDER BY c_first) AS rn
              FROM customer
             WHERE c_w_id = p_w_id AND c_d_id = p_d_id AND c_last = p_c_last
          ) sub WHERE sub.rn = v_match_pos;
    ELSE
        SELECT c_balance, c_first, c_middle, c_last
          INTO v_c_balance, v_c_first, v_c_middle, v_c_last
          FROM customer
         WHERE c_w_id = p_w_id AND c_d_id = p_d_id AND c_id = p_c_id;
        v_c_id := p_c_id;
    END IF;

    SELECT o_id, o_carrier_id, o_entry_d
      INTO v_o_id, v_o_carrier_id, v_o_entry_d
      FROM orders
     WHERE o_w_id = p_w_id AND o_d_id = p_d_id AND o_c_id = v_c_id
     ORDER BY o_id DESC LIMIT 1;

    -- Drain order_line rows for the latest order so the engine pays the
    -- same scan cost as the statement-mode version.
    FOR r IN
        SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
          FROM order_line
         WHERE ol_w_id = p_w_id AND ol_d_id = p_d_id AND ol_o_id = v_o_id
    LOOP
        NULL;
    END LOOP;
END;
$$;
`

// tpcc_delivery(p_w_id, p_o_carrier_id, p_ol_delivery_d TIMESTAMP)
//
// For each of 10 districts in p_w_id: pick the oldest new_order row,
// delete it, set the order's carrier_id, mark each order_line as delivered
// with p_ol_delivery_d, sum order_line amounts and credit customer balance.
const procDelivery = `
CREATE OR REPLACE PROCEDURE tpcc_delivery(
    p_w_id           INT,
    p_o_carrier_id   INT,
    p_ol_delivery_d  TIMESTAMP
) LANGUAGE plpgsql AS $$
DECLARE
    d_id     INT;
    v_o_id   INT;
    v_c_id   INT;
    v_amount NUMERIC;
BEGIN
    FOR d_id IN 1..10 LOOP
        SELECT no_o_id INTO v_o_id
          FROM new_order
         WHERE no_w_id = p_w_id AND no_d_id = d_id
         ORDER BY no_o_id ASC LIMIT 1
           FOR UPDATE;
        IF NOT FOUND THEN
            CONTINUE;
        END IF;

        DELETE FROM new_order
         WHERE no_w_id = p_w_id AND no_d_id = d_id AND no_o_id = v_o_id;

        UPDATE orders SET o_carrier_id = p_o_carrier_id
         WHERE o_w_id = p_w_id AND o_d_id = d_id AND o_id = v_o_id
        RETURNING o_c_id INTO v_c_id;

        UPDATE order_line SET ol_delivery_d = p_ol_delivery_d
         WHERE ol_w_id = p_w_id AND ol_d_id = d_id AND ol_o_id = v_o_id;

        SELECT SUM(ol_amount) INTO v_amount
          FROM order_line
         WHERE ol_w_id = p_w_id AND ol_d_id = d_id AND ol_o_id = v_o_id;

        UPDATE customer
           SET c_balance       = c_balance + v_amount,
               c_delivery_cnt  = c_delivery_cnt + 1
         WHERE c_w_id = p_w_id AND c_d_id = d_id AND c_id = v_c_id;
    END LOOP;
END;
$$;
`

// tpcc_stock_level(p_w_id, p_d_id, p_threshold)
//
// Pure read. Number of distinct s_i_id with low stock among the last 20
// order_line rows of the district.
const procStockLevel = `
CREATE OR REPLACE PROCEDURE tpcc_stock_level(
    p_w_id      INT,
    p_d_id      INT,
    p_threshold INT
) LANGUAGE plpgsql AS $$
DECLARE
    v_o_id      INT;
    v_stock_cnt INT;
BEGIN
    SELECT d_next_o_id INTO v_o_id
      FROM district WHERE d_w_id = p_w_id AND d_id = p_d_id;

    SELECT COUNT(DISTINCT s_i_id) INTO v_stock_cnt
      FROM order_line, stock
     WHERE ol_w_id   = p_w_id
       AND ol_d_id   = p_d_id
       AND ol_o_id   <  v_o_id
       AND ol_o_id   >= v_o_id - 20
       AND s_w_id    = p_w_id
       AND s_i_id    = ol_i_id
       AND s_quantity < p_threshold;
END;
$$;
`

// allStoredProcs is consumed by installStoredProcsPG: each element is a
// standalone CREATE OR REPLACE PROCEDURE statement.
var allStoredProcs = []string{
	procNewOrder,
	procPayment,
	procOrderStatus,
	procDelivery,
	procStockLevel,
}

// installStoredProcsPG runs all the CREATE OR REPLACE PROCEDURE statements.
// Safe to invoke repeatedly — every one of them is idempotent.
func installStoredProcsPG(ctx context.Context, conn *sql.Conn) error {
	for _, ddl := range allStoredProcs {
		if _, err := conn.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("install stored procedure failed: %w\nDDL:\n%s", err, ddl)
		}
	}
	return nil
}
