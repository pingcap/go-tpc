package dtable

import (
	"database/sql"
	sqldrv "database/sql/driver"
	"net/url"
)

type Driver struct {
}

func (d *Driver) Open(name string) (sqldrv.Conn, error) {
	u, err := url.Parse(name)
	if err != nil {
		return nil, err
	}
	baseId := u.Query().Get("baseId")
	privateKey := u.Query().Get("privateKey")

	cli, err := newClient(baseId, &url.URL{Host: u.Host, Scheme: u.Scheme}, privateKey)

	return &dtableConn{baseId: baseId, privateKey: privateKey, cli: cli}, nil
}

const driverName = "dtable"

func init() {
	sql.Register(driverName, &Driver{})
}
