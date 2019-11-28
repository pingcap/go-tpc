package tpch

import (
	"context"
	"fmt"
)

func (w *Workloader) createTableDDL(ctx context.Context, query string, tableName string, action string) error {
	s := w.getState(ctx)
	fmt.Printf("%s %s\n", action, tableName)
	if _, err := s.Conn.ExecContext(ctx, query); err != nil {
		return err
	}
	return nil
}

func (w *Workloader) createTable(ctx context.Context) error {
	query := `
CREATE TABLE IF NOT EXISTS nation (
    N_NATIONKEY BIGINT NOT NULL,
    N_NAME CHAR(25) NOT NULL,
    N_REGIONKEY BIGINT NOT NULL,
    N_COMMENT VARCHAR(152),
    PRIMARY KEY (N_NATIONKEY)
)`

	if err := w.createTableDDL(ctx, query, "nation", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS region (
    R_REGIONKEY BIGINT NOT NULL,
    R_NAME CHAR(25) NOT NULL,
    R_COMMENT VARCHAR(152),
    PRIMARY KEY (R_REGIONKEY)
)`
	if err := w.createTableDDL(ctx, query, "region", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS part (
    P_PARTKEY BIGINT NOT NULL,
    P_NAME VARCHAR(55) NOT NULL,
    P_MFGR CHAR(25) NOT NULL,
    P_BRAND CHAR(10) NOT NULL,
    P_TYPE VARCHAR(25) NOT NULL,
    P_SIZE BIGINT NOT NULL,
    P_CONTAINER CHAR(10) NOT NULL,
    P_RETAILPRICE DECIMAL(15, 2) NOT NULL,
    P_COMMENT VARCHAR(23) NOT NULL,
    PRIMARY KEY (P_PARTKEY)
)`
	if err := w.createTableDDL(ctx, query, "part", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS supplier (
    S_SUPPKEY BIGINT NOT NULL,
    S_NAME CHAR(25) NOT NULL,
    S_ADDRESS VARCHAR(40) NOT NULL,
    S_NATIONKEY BIGINT NOT NULL,
    S_PHONE CHAR(15) NOT NULL,
    S_ACCTBAL DECIMAL(15, 2) NOT NULL,
    S_COMMENT VARCHAR(101) NOT NULL,
    PRIMARY KEY (S_SUPPKEY)
)`
	if err := w.createTableDDL(ctx, query, "supplier", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS partsupp (
    PS_PARTKEY BIGINT NOT NULL,
    PS_SUPPKEY BIGINT NOT NULL,
    PS_AVAILQTY BIGINT NOT NULL,
    PS_SUPPLYCOST DECIMAL(15, 2) NOT NULL,
    PS_COMMENT VARCHAR(199) NOT NULL,
    PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
)`
	if err := w.createTableDDL(ctx, query, "partsupp", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS customer (
    C_CUSTKEY BIGINT NOT NULL,
    C_NAME VARCHAR(25) NOT NULL,
    C_ADDRESS VARCHAR(40) NOT NULL,
    C_NATIONKEY BIGINT NOT NULL,
    C_PHONE CHAR(15) NOT NULL,
    C_ACCTBAL DECIMAL(15, 2) NOT NULL,
    C_MKTSEGMENT CHAR(10) NOT NULL,
    C_COMMENT VARCHAR(117) NOT NULL,
    PRIMARY KEY (C_CUSTKEY)
)`
	if err := w.createTableDDL(ctx, query, "customer", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS orders (
    O_ORDERKEY BIGINT NOT NULL,
    O_CUSTKEY BIGINT NOT NULL,
    O_ORDERSTATUS CHAR(1) NOT NULL,
    O_TOTALPRICE DECIMAL(15, 2) NOT NULL,
    O_ORDERDATE DATE NOT NULL,
    O_ORDERPRIORITY CHAR(15) NOT NULL,
    O_CLERK CHAR(15) NOT NULL,
    O_SHIPPRIORITY BIGINT NOT NULL,
    O_COMMENT VARCHAR(79) NOT NULL,
    PRIMARY KEY (O_ORDERKEY)
)`
	if err := w.createTableDDL(ctx, query, "orders", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS lineitem (
    L_ORDERKEY BIGINT NOT NULL,
    L_PARTKEY BIGINT NOT NULL,
    L_SUPPKEY BIGINT NOT NULL,
    L_LINENUMBER BIGINT NOT NULL,
    L_QUANTITY DECIMAL(15, 2) NOT NULL,
    L_EXTENDEDPRICE DECIMAL(15, 2) NOT NULL,
    L_DISCOUNT DECIMAL(15, 2) NOT NULL,
    L_TAX DECIMAL(15, 2) NOT NULL,
    L_RETURNFLAG CHAR(1) NOT NULL,
    L_LINESTATUS CHAR(1) NOT NULL,
    L_SHIPDATE DATE NOT NULL,
    L_COMMITDATE DATE NOT NULL,
    L_RECEIPTDATE DATE NOT NULL,
    L_SHIPINSTRUCT CHAR(25) NOT NULL,
    L_SHIPMODE CHAR(10) NOT NULL,
    L_COMMENT VARCHAR(44) NOT NULL,
    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
)
`
	if err := w.createTableDDL(ctx, query, "lineitem", "creating"); err != nil {
		return err
	}
	return nil
}

func (w *Workloader) dropTable(ctx context.Context) error {
	s := w.getState(ctx)
	tables := []string{
		"lineitem", "partsupp", "supplier", "part", "orders", "customer", "region", "nation",
	}

	for _, tbl := range tables {
		fmt.Printf("DROP TABLE IF EXISTS %s\n", tbl)
		if _, err := s.Conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl)); err != nil {
			return err
		}
	}
	return nil
}
