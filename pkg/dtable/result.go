package dtable

type dtableResult struct {
}

func (d *dtableResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (d *dtableResult) RowsAffected() (int64, error) {
	return 0, nil
}
