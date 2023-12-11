package dtable

import (
	"context"
	sqldrv "database/sql/driver"
)

type dtableStmt struct {
	conn  *dtableConn
	query string
}

func (d *dtableStmt) QueryContext(ctx context.Context, args []sqldrv.NamedValue) (sqldrv.Rows, error) {
	return d.conn.QueryContext(ctx, d.query, args)
}

func (d *dtableStmt) ExecContext(ctx context.Context, args []sqldrv.NamedValue) (sqldrv.Result, error) {
	return d.conn.ExecContext(ctx, d.query, args)
}

func (d *dtableStmt) Close() error {
	return nil
}

func (d *dtableStmt) NumInput() int {
	// we don't know param count
	return -1
}

func (d *dtableStmt) Exec(args []sqldrv.Value) (sqldrv.Result, error) {
	return d.conn.Exec(d.query, args)
}

func (d *dtableStmt) Query(args []sqldrv.Value) (sqldrv.Rows, error) {
	return d.conn.Query(d.query, args)
}
