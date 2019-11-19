package tpch

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pingcap/go-tpc/pkg/measurement"
	"github.com/pingcap/go-tpc/pkg/workload"
	"time"
)

type contextKey string

const stateKey = contextKey("tpch")

// Config is the configuration for tpch workload
type Config struct {
	RawQueries string
	QueryNames []string
}

type tpchState struct {
	*workload.TpcState
	queryIdx int
}

// Workloader is TPCH workload
type Workloader struct {
	db  *sql.DB
	cfg *Config
}

func NewWorkloader(db *sql.DB, cfg *Config) workload.Workloader {
	return Workloader{
		db:  db,
		cfg: cfg,
	}
}

func (w *Workloader) getState(ctx context.Context) *tpchState {
	s := ctx.Value(stateKey).(*tpchState)
	return s
}

func (w *Workloader) updateState(ctx context.Context) {
	s := w.getState(ctx)
	s.queryIdx += 1
}

func (w Workloader) Name() string {
	return "tpch"
}

func (w Workloader) InitThread(ctx context.Context, threadID int) context.Context {
	s := &tpchState{
		queryIdx: threadID % len(w.cfg.QueryNames),
		TpcState: workload.NewTpcState(ctx, w.db),
	}
	ctx = context.WithValue(ctx, stateKey, s)

	return ctx
}

func (w Workloader) CleanupThread(ctx context.Context, threadID int) {
}

func (w Workloader) Prepare(ctx context.Context, threadID int) error {
	panic("implement me")
}

func (w Workloader) CheckPrepare(ctx context.Context, threadID int) error {
	panic("implement me")
}

func (w Workloader) Run(ctx context.Context, threadID int) error {
	s := w.getState(ctx)
	defer w.updateState(ctx)

	queryName := w.cfg.QueryNames[s.queryIdx%len(w.cfg.QueryNames)]
	query := queries[queryName]

	start := time.Now()
	rows, err := s.Conn.QueryContext(ctx, query)
	measurement.Measure(queryName, time.Now().Sub(start), err)

	if err != nil {
		return fmt.Errorf("execute %s failed %v", queryName, err)
	}

	if err := w.checkQueryResult(queryName, rows); err != nil {
		return fmt.Errorf("check %s failed %v", queryName, err)
	}
	return nil
}

func (w Workloader) Cleanup(ctx context.Context, threadID int) error {
	panic("implement me")
}

func (w Workloader) Check(ctx context.Context, threadID int) error {
	panic("implement me")
}

func (w Workloader) checkQueryResult(queryName string, rows *sql.Rows) error {
	// TODO
	defer rows.Close()
	return nil
}
