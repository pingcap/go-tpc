package dtable

type dtableTxn struct {
	conn *dtableConn
}

func (t *dtableTxn) Commit() error {
	return t.conn.commit()
}

func (t *dtableTxn) Rollback() error {
	return t.conn.rollback()
}
