package workload

import (
	"context"
	"database/sql"
	"math/rand"
	"time"

	"github.com/pingcap/go-tpc/pkg/util"
)

// TpcState saves state for each thread
type TpcState struct {
	DB   *sql.DB
	Conn *sql.Conn

	R *rand.Rand

	Buf *util.BufAllocator
}

func (t *TpcState) RereshConn(ctx context.Context) error {
	conn, err := t.DB.Conn(ctx)
	if err != nil {
		return err
	}
	t.Conn = conn
	return nil
}

// NewTpcState creates a base TpcState
func NewTpcState(ctx context.Context, db *sql.DB) *TpcState {
	var conn *sql.Conn
	var err error
	if db != nil {
		conn, err = db.Conn(ctx)
		if err != nil {
			panic(err.Error())
		}
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	s := &TpcState{
		DB:   db,
		Conn: conn,
		R:    r,
		Buf:  util.NewBufAllocator(),
	}
	return s
}
