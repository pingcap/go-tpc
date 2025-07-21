package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

func prepareStmt(ctx context.Context, conn *sql.Conn, query string) *sql.Stmt {
	stmt, err := conn.PrepareContext(ctx, query)
	if err != nil {
		fmt.Println(fmt.Sprintf("prepare statement error: %s", query))
		panic(err)
	}
	return stmt
}

func newOrder(conn *sql.Conn) {
	ctx := context.Background()
	stmt1 := prepareStmt(ctx, conn, newOrderSelectCustomer)

	fmt.Println("begin txn")
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.IsolationLevel(0)})
	if err != nil {
		panic(err)
	}

	var (
		cDiscount float64
		cLast     string
		cCredit   []byte
		wTax      float64
	)

	fmt.Println("query q1")

	if err := stmt1.QueryRowContext(ctx, 1, 1, 1).Scan(&cDiscount, &cLast, &cCredit, &wTax); err != nil {
		panic(err)
	}

	fmt.Println("commit txn")
	if err := tx.Commit(); err != nil {
		panic(err)
	}
}

func main() {
	fmt.Println("start")
	db, err := sql.Open("mysql", "root:@tcp(localhost:4001)/test")
	fmt.Println("err ", err)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("conn ", conn)

	newOrder(conn)
}

const (
	newOrderSelectCustomer = `SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?`
	newOrderSelectDistrict = `SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ? FOR UPDATE`
	newOrderUpdateDistrict = `UPDATE district SET d_next_o_id = ? + 1 WHERE d_id = ? AND d_w_id = ?`
	newOrderInsertOrder    = `INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?)`
	newOrderInsertNewOrder = `INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)`
	newOrderUpdateStock    = `UPDATE stock SET s_quantity = ?, s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + ? WHERE s_i_id = ? AND s_w_id = ?`
)

/*
 tiup playground --tag "TPCC" --db 1 --kv 1 --tiflash 0 v8.5.1
 tiup playground --tag "TPCC" --db 1 --db.binpath='/Users/zhangyuanjia/Workspace/go/src/git.pingcap.net/pingkai/tidb/bin/tidb-server' --kv 1 --tiflash 0 v8.5.1
*/
