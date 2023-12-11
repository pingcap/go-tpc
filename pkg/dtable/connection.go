package dtable

import (
	"context"
	sqldrv "database/sql/driver"
	"sync"
	"time"
)

type dtableConn struct {
	serverAddr string
	baseId     string
	privateKey string // for jwt token
	cli        *client

	lock    sync.RWMutex // for txn state
	txnMode bool
	txnId   string
}

func (d *dtableConn) Ping(_ context.Context) error {
	return nil
}

const timeOut = time.Minute * 1

func (d *dtableConn) QueryContext(ctx context.Context, query string, args []sqldrv.NamedValue) (sqldrv.Rows, error) {
	ctx, cancel := context.WithTimeout(ctx, timeOut)
	defer cancel()

	params := make([]sqldrv.Value, len(args))
	for i := range args {
		params[i] = args[i].Value
	}

	d.lock.RLock()
	defer d.lock.RUnlock()
	if d.txnMode {
		return d.cli.queryTxn(ctx, d.txnId, query, params)
	} else {
		return d.cli.query(ctx, query, params)
	}
}

func (d *dtableConn) Query(query string, args []sqldrv.Value) (sqldrv.Rows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()

	d.lock.RLock()
	defer d.lock.RUnlock()
	if d.txnMode {
		return d.cli.queryTxn(ctx, d.txnId, query, args)
	} else {
		return d.cli.query(ctx, query, args)
	}
}

func (d *dtableConn) Exec(query string, args []sqldrv.Value) (sqldrv.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()

	d.lock.RLock()
	defer d.lock.RUnlock()
	if d.txnMode {
		return &dtableResult{}, d.cli.execTxn(ctx, d.txnId, query, args)
	} else {
		return &dtableResult{}, d.cli.exec(ctx, query, args)
	}
}

func (d *dtableConn) ExecContext(ctx context.Context, query string, args []sqldrv.NamedValue) (sqldrv.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, timeOut)
	defer func() { cancel() }()

	params := make([]sqldrv.Value, len(args))
	for i := range args {
		params[i] = args[i].Value
	}

	d.lock.RLock()
	defer d.lock.RUnlock()
	if d.txnMode {
		return &dtableResult{}, d.cli.execTxn(ctx, d.txnId, query, params)
	} else {
		return &dtableResult{}, d.cli.exec(ctx, query, params)
	}
}

func (d *dtableConn) Prepare(query string) (sqldrv.Stmt, error) {
	return &dtableStmt{conn: d, query: query}, nil
}

func (d *dtableConn) PrepareContext(ctx context.Context, query string) (sqldrv.Stmt, error) {
	return &dtableStmt{conn: d, query: query}, nil
}

func (d *dtableConn) Close() error {
	return nil
}

func (d *dtableConn) Begin() (sqldrv.Tx, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()

	d.lock.Lock()
	defer d.lock.Unlock()
	txnId, err := d.cli.beginTxn(ctx)
	if err != nil {
		return nil, err
	}
	d.txnMode = true
	d.txnId = txnId

	return &dtableTxn{conn: d}, err
}

func (d *dtableConn) BeginTx(ctx context.Context, _ sqldrv.TxOptions) (sqldrv.Tx, error) {
	ctx, cancel := context.WithTimeout(ctx, timeOut)
	defer cancel()

	d.lock.Lock()
	defer d.lock.Unlock()
	if d.txnMode {
		return &dtableTxn{conn: d}, nil
	}

	txnId, err := d.cli.beginTxn(ctx)
	if err != nil {
		return nil, err
	}
	d.txnMode = true
	d.txnId = txnId
	return &dtableTxn{conn: d}, nil
}

func (d *dtableConn) commit() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()

	d.lock.Lock()
	defer d.lock.Unlock()
	if !d.txnMode {
		return nil
	}

	err := d.cli.commitTxn(ctx, d.txnId)
	d.txnMode = false
	d.txnId = ""
	return err
}

func (d *dtableConn) rollback() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()

	d.lock.Lock()
	defer d.lock.Unlock()
	if !d.txnMode {
		return nil
	}

	err := d.cli.rollBackTxn(ctx, d.txnId)
	d.txnMode = false
	d.txnId = ""
	return err
}
