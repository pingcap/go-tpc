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
	Conn *sql.Conn

	R *rand.Rand

	Buf *util.BufAllocator
}

// NewTpcState creates a base TpcState
func NewTpcState(ctx context.Context, db *sql.DB) *TpcState {
	conn, err := db.Conn(ctx)
	if err != nil {
		panic(err.Error())
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	s := &TpcState{
		Conn: conn,
		R:    r,
		Buf:  util.NewBufAllocator(),
	}
	return s
}
