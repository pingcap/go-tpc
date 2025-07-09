package tpch

import (
	"context"
	"fmt"
)

const (
	lineitemStmt = `
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
)`

	partsuppStmt = `
CREATE TABLE IF NOT EXISTS partsupp (
	PS_PARTKEY BIGINT NOT NULL,
	PS_SUPPKEY BIGINT NOT NULL,
	PS_AVAILQTY BIGINT NOT NULL,
	PS_SUPPLYCOST DECIMAL(15, 2) NOT NULL,
	PS_COMMENT VARCHAR(199) NOT NULL,
	PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
)`

	supplierStmt = `
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

	partStmt = `
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

	ordersStmt = `
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

	customerStmt = `
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

	regionStmt = `
CREATE TABLE IF NOT EXISTS region (
    R_REGIONKEY BIGINT NOT NULL,
    R_NAME CHAR(25) NOT NULL,
    R_COMMENT VARCHAR(152),
    PRIMARY KEY (R_REGIONKEY)
)`

	nationStmt = `
CREATE TABLE IF NOT EXISTS nation (
    N_NATIONKEY BIGINT NOT NULL,
    N_NAME CHAR(25) NOT NULL,
    N_REGIONKEY BIGINT NOT NULL,
    N_COMMENT VARCHAR(152),
    PRIMARY KEY (N_NATIONKEY)
)`
)

var allTables []string
var tableMap map[string]string

func init() {
	allTables = []string{"lineitem", "partsupp", "supplier", "part", "orders", "customer", "region", "nation"}
	tableMap = map[string]string{
		"lineitem": lineitemStmt,
		"partsupp": partsuppStmt,
		"supplier": supplierStmt,
		"part":     partStmt,
		"orders":   ordersStmt,
		"customer": customerStmt,
		"region":   regionStmt,
		"nation":   nationStmt,
	}
}

// createTables creates tables schema.
func (w *Workloader) createTables(ctx context.Context) error {
	s := w.getState(ctx)
	for tableName, tableStmt := range tableMap {
		fmt.Printf("%s %s\n", "creating", tableName)
		if _, err := s.Conn.ExecContext(ctx, tableStmt); err != nil {
			return err
		}
		if w.cfg.TiFlashReplica != 0 {
			fmt.Printf("creating tiflash replica for %s\n", tableName)
			replicaSQL := fmt.Sprintf("ALTER TABLE %s SET TIFLASH REPLICA %d", tableName, w.cfg.TiFlashReplica)
			if _, err := s.Conn.ExecContext(ctx, replicaSQL); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Workloader) dropTables(ctx context.Context) error {
	s := w.getState(ctx)

	for _, tbl := range allTables {
		fmt.Printf("DROP TABLE IF EXISTS %s\n", tbl)
		if _, err := s.Conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl)); err != nil {
			return err
		}
	}
	return nil
}

func (w *Workloader) createInvertedIndexes(ctx context.Context) error {
	indexes := map[string]string{
		"idx_l_receiptdate": "CREATE COLUMNAR INDEX IF NOT EXISTS idx_l_receiptdate ON lineitem (l_receiptdate) USING INVERTED",
		"idx_l_shipdate":    "CREATE COLUMNAR INDEX IF NOT EXISTS idx_l_shipdate ON lineitem (l_shipdate) USING INVERTED",
		"idx_o_orderdate":   "CREATE COLUMNAR INDEX IF NOT EXISTS idx_o_orderdate ON orders (o_orderdate) USING INVERTED",
	}
	s := w.getState(ctx)
	for name, stmt := range indexes {
		fmt.Printf("creating inverted index %s\n", name)
		if _, err := s.Conn.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
