package tpcc

import (
	"context"
	"fmt"
)

func (w *Workloader) createTableDDL(ctx context.Context, query string, tableID int, tableName string, action string) error {
	s := w.base.GetState(ctx)
	fmt.Printf("%s %s%d\n", action, tableName, tableID)
	if _, err := s.Conn.ExecContext(ctx, fmt.Sprintf(query, tableID)); err != nil {
		return err
	}
	return nil
}

func (w *Workloader) createTable(ctx context.Context, tableID int) error {
	// Warehouse
	query := `
CREATE TABLE IF NOT EXISTS warehouse%d (
	w_id INT NOT NULL,
	w_name VARCHAR(10),
	w_street_1 VARCHAR(20),
	w_street_2 VARCHAR(20),
	w_city VARCHAR(20),
	w_state CHAR(2),
	w_zip CHAR(9),
	w_tax DECIMAL(4, 4),
	w_ytd DECIMAL(12, 2),
	PRIMARY KEY (w_id)
)`

	if err := w.createTableDDL(ctx, query, tableID, "warehouse", "creating"); err != nil {
		return err
	}

	// District
	query = `
CREATE TABLE IF NOT EXISTS district%d (
	d_id INT NOT NULL,
	d_w_id INT NOT NULL,
	d_name VARCHAR(10),
	d_street_1 VARCHAR(20),
	d_street_2 VARCHAR(20),
	d_city VARCHAR(20),
	d_state CHAR(2),
	d_zip CHAR(9),
	d_tax DECIMAL(4, 4),
	d_ytd DECIMAL(12, 2),
	d_next_o_id INT,
	PRIMARY KEY (d_w_id, d_id)
)`

	if err := w.createTableDDL(ctx, query, tableID, "district", "creating"); err != nil {
		return err
	}

	// Customer
	query = `
CREATE TABLE IF NOT EXISTS customer%d (
	c_id INT NOT NULL, 
	c_d_id INT NOT NULL,
	c_w_id INT NOT NULL, 
	c_first VARCHAR(16), 
	c_middle CHAR(2), 
	c_last VARCHAR(16), 
	c_street_1 VARCHAR(20), 
	c_street_2 VARCHAR(20), 
	c_city VARCHAR(20), 
	c_state CHAR(2), 
	c_zip CHAR(9), 
	c_phone CHAR(16), 
	c_since TIMESTAMP, 
	c_credit CHAR(2), 
	c_credit_lim DECIMAL(12, 2), 
	c_discount DECIMAL(4,4), 
	c_balance DECIMAL(12,2), 
	c_ytd_payment DECIMAL(12,2), 
	c_payment_cnt INT, 
	c_delivery_cnt INT, 
	c_data VARCHAR(500),
	PRIMARY KEY(c_w_id, c_d_id, c_id),
	INDEX idx_customer (c_w_id, c_d_id, c_last, c_first)
)`

	if err := w.createTableDDL(ctx, query, tableID, "customer", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS history%d (
	id INT NOT NULL AUTO_INCREMENT,
	h_c_id INT NOT NULL,
	h_c_d_id INT NOT NULL,
	h_c_w_id INT NOT NULL,
	h_d_w_id INT NOT NULL,
	h_d_id INT NOT NULL,
	h_w_id INT NOT NULL,
	h_date TIMESTAMP,
	h_amount DECIMAL(6, 2),
	h_data VARCHAR(24),
	PRIMARY KEY(id),
	INDEX idx_history_customer (h_c_w_id, h_c_d_id, h_c_id),
	INDEX idx_history_district (h_w_id, h_d_id)
)`

	if err := w.createTableDDL(ctx, query, tableID, "history", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS new_order%d (
	no_o_id INT NOT NULL,
	no_d_id INT NOT NULL,
	no_w_id INT NOT NULL,
	PRIMARY KEY(no_w_id, no_d_id, no_o_id)
)`

	if err := w.createTableDDL(ctx, query, tableID, "new_order", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS order%d (
	o_id INT NOT NULL,
	o_d_id INT NOT NULL,
	o_w_id INT NOT NULL,
	o_c_id INT,
	o_entry_d TIMESTAMP,
	o_carrier_id INT,
	o_ol_cnt INT,
	o_all_local INT,
	PRIMARY KEY(o_w_id, o_d_id, o_id),
	INDEX idx_order (o_w_id, o_d_id, o_c_id, o_id)
)`

	if err := w.createTableDDL(ctx, query, tableID, "order", "creating"); err != nil {
		return err
	}

	query = `
	CREATE TABLE IF NOT EXISTS order_line%d (
		ol_o_id INT NOT NULL,
		ol_d_id INT NOT NULL,
		ol_w_id INT NOT NULL,
		ol_number INT NOT NULL,
		ol_l_id INT NOT NULL,
		ol_supply_w_id INT,
		ol_delivery_d TIMESTAMP,
		ol_quantity INT,
		ol_amount DECIMAL(6, 2),
		ol_dist_info CHAR(24),
		PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number),
		INDEX idx_order_line_stock (ol_supply_w_id, ol_d_id)
)`

	if err := w.createTableDDL(ctx, query, tableID, "order_line", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS item%d (
	i_id INT NOT NULL,
	i_im_id INT,
	i_name VARCHAR(24),
	i_price DECIMAL(5, 2),
	i_data VARCHAR(50),
	PRIMARY KEY(i_id)
)`

	if err := w.createTableDDL(ctx, query, tableID, "item", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS stock%d (
	s_i_id INT NOT NULL,
	s_w_id INT NOT NULL,
	s_quantity INT,
	s_dist_01 CHAR(24), 
	s_dist_02 CHAR(24),
	s_dist_03 CHAR(24),
	s_dist_04 CHAR(24), 
	s_dist_05 CHAR(24), 
	s_dist_06 CHAR(24), 
	s_dist_07 CHAR(24), 
	s_dist_08 CHAR(24), 
	s_dist_09 CHAR(24), 
	s_dist_10 CHAR(24), 
	s_ytd INT, 
	s_order_cnt INT, 
	s_remote_cnt INT,
	s_data VARCHAR(50),
	PRIMARY KEY(s_w_id, s_i_id),
	INDEX idx_stock_item (s_i_id)
)`

	if err := w.createTableDDL(ctx, query, tableID, "stock", "creating"); err != nil {
		return err
	}

	if w.cfg.UseFK {
		// TODO: Add foreign key constraint
	}

	return nil
}

func (w *Workloader) dropTable(ctx context.Context, tableID int) error {
	s := w.base.GetState(ctx)
	tables := []string{
		"warehouse", "history", "new_order", "order_line", "order", "customer", "district", "stock", "item",
	}

	for _, tbl := range tables {
		fmt.Printf("dropping %s%d\n", tbl, tableID)
		if _, err := s.Conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s%d", tbl, tableID)); err != nil {
			return err
		}
	}

	return nil
}
