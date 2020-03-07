package tpch

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pingcap/go-tpc/tpch/dbgen"

	"github.com/pingcap/go-tpc/pkg/measurement"
	"github.com/pingcap/go-tpc/pkg/workload"
)

type contextKey string

const stateKey = contextKey("tpch")

// Config is the configuration for tpch workload
type Config struct {
	DBName            string
	RawQueries        string
	QueryNames        []string
	ScaleFactor       int
	EnableOutputCheck bool
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

// NewWorkloader new work loader
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
	s.queryIdx++
}

// Name return workloader name
func (w Workloader) Name() string {
	return "tpch"
}

// InitThread inits thread
func (w Workloader) InitThread(ctx context.Context, threadID int) context.Context {
	s := &tpchState{
		queryIdx: threadID % len(w.cfg.QueryNames),
		TpcState: workload.NewTpcState(ctx, w.db),
	}
	ctx = context.WithValue(ctx, stateKey, s)

	return ctx
}

// CleanupThread cleans up thread
func (w Workloader) CleanupThread(ctx context.Context, threadID int) {
	s := w.getState(ctx)
	s.Conn.Close()
}

// Prepare prepares data
func (w Workloader) Prepare(ctx context.Context, threadID int) error {
	if threadID != 0 {
		return nil
	}
	s := w.getState(ctx)

	if err := w.createTables(ctx); err != nil {
		return err
	}
	sqlLoader := map[dbgen.Table]dbgen.Loader{
		dbgen.TOrder:  newOrderLoader(ctx, s.Conn),
		dbgen.TLine:   newLineItemLoader(ctx, s.Conn),
		dbgen.TPart:   newPartLoader(ctx, s.Conn),
		dbgen.TPsupp:  newPartSuppLoader(ctx, s.Conn),
		dbgen.TSupp:   newSuppLoader(ctx, s.Conn),
		dbgen.TCust:   newCustLoader(ctx, s.Conn),
		dbgen.TNation: newNationLoader(ctx, s.Conn),
		dbgen.TRegion: newRegionLoader(ctx, s.Conn),
	}
	dbgen.InitDbGen(int64(w.cfg.ScaleFactor))
	return dbgen.DbGen(sqlLoader)
}

// CheckPrepare checks prepare
func (w Workloader) CheckPrepare(ctx context.Context, threadID int) error {
	return nil
}

// Run runs workload
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

	// we only check scale = 1, it was much quick
	if w.cfg.ScaleFactor == 1 && w.cfg.EnableOutputCheck {
		if err := w.checkQueryResult(queryName, rows); err != nil {
			return fmt.Errorf("check %s failed %v", queryName, err)
		}
	}
	return nil
}

// Cleanup cleans up workloader
func (w Workloader) Cleanup(ctx context.Context, threadID int) error {
	if threadID != 0 {
		return nil
	}
	return w.dropTable(ctx)
}

// Check checks data
func (w Workloader) Check(ctx context.Context, threadID int) error {
	return nil
}

// DataGen returns a bool to represent whether to generate csv data or load data to db.
func (w Workloader) DataGen() bool {
	return false
}

// DBName returns the name of test db.
func (w Workloader) DBName() string {
	return w.cfg.DBName
}
