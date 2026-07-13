package tpcc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync/atomic"
	"testing"
)

type trackingConnector struct {
	closeCount *atomic.Int32
}

func (c trackingConnector) Connect(context.Context) (driver.Conn, error) {
	return trackingConn{closeCount: c.closeCount}, nil
}

func (trackingConnector) Driver() driver.Driver {
	return trackingDriver{}
}

type trackingDriver struct{}

func (trackingDriver) Open(string) (driver.Conn, error) {
	return nil, driver.ErrBadConn
}

type trackingConn struct {
	closeCount *atomic.Int32
}

func (c trackingConn) Prepare(string) (driver.Stmt, error) {
	return trackingStmt{closeCount: c.closeCount}, nil
}

func (trackingConn) Close() error {
	return nil
}

func (trackingConn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

type trackingStmt struct {
	closeCount *atomic.Int32
}

func (s trackingStmt) Close() error {
	s.closeCount.Add(1)
	return nil
}

func (trackingStmt) NumInput() int {
	return -1
}

func (trackingStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, driver.ErrSkip
}

func (trackingStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, driver.ErrSkip
}

func TestClosePreparedStatementsClosesAndClearsStatementMaps(t *testing.T) {
	var closeCount atomic.Int32
	db := sql.OpenDB(trackingConnector{closeCount: &closeCount})
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	prepareStmt := func(query string) *sql.Stmt {
		stmt, err := conn.PrepareContext(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		return stmt
	}

	s := &tpccState{
		newOrderStmts:    map[string]*sql.Stmt{"new_order": prepareStmt("new_order")},
		orderStatusStmts: map[string]*sql.Stmt{"order_status": prepareStmt("order_status")},
		deliveryStmts:    map[string]*sql.Stmt{"delivery": prepareStmt("delivery")},
		stockLevelStmt:   map[string]*sql.Stmt{"stock_level": prepareStmt("stock_level")},
		paymentStmts:     map[string]*sql.Stmt{"payment": prepareStmt("payment")},
	}

	s.closePreparedStatements()

	if got := closeCount.Load(); got != 5 {
		t.Fatalf("expected 5 prepared statements to close, got %d", got)
	}
	if s.newOrderStmts != nil || s.orderStatusStmts != nil || s.deliveryStmts != nil ||
		s.stockLevelStmt != nil || s.paymentStmts != nil {
		t.Fatal("prepared statement maps must be cleared after closing")
	}
}
