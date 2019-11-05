package workload

import (
	"context"
	"database/sql"
	"math/rand"
	"time"

	"github.com/siddontang/go-tpc/pkg/util"
)

type contextKey string

const stateKey = contextKey("tpc")

// TpcState saves state for each thread
type TpcState struct {
	Conn *sql.Conn

	R *rand.Rand

	Buf *util.BufAllocator
}

// BaseWorkloader is a base workloader for other TPC workloaders depend on
type BaseWorkloader struct {
	DB *sql.DB
}

// InitThread implements Workloader interface
func (w BaseWorkloader) InitThread(ctx context.Context, threadID int) context.Context {
	conn, err := w.DB.Conn(ctx)
	if err != nil {
		panic(err.Error())
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	s := &TpcState{
		Conn: conn,
		R:    r,
		Buf:  util.NewBufAllocator(),
	}

	return context.WithValue(ctx, stateKey, s)
}

// CleanupThread implements Workloader interface
func (w BaseWorkloader) CleanupThread(ctx context.Context, threadID int) {
	s := ctx.Value(stateKey).(*TpcState)
	s.Conn.Close()
}

// GetState gets the base state
func (w BaseWorkloader) GetState(ctx context.Context) *TpcState {
	return ctx.Value(stateKey).(*TpcState)
}
