package rawsql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/go-tpc/pkg/measurement"
	"github.com/pingcap/go-tpc/pkg/util"
	"github.com/pingcap/go-tpc/pkg/workload"
)

type contextKey string

const stateKey = contextKey("rawsql")

type Config struct {
	DBName             string
	Queries            map[string]string // query name: query SQL
	QueryNames         []string
	ExecExplainAnalyze bool
	RefreshWait        time.Duration
}

type rawsqlState struct {
	queryIdx int
	*workload.TpcState
}

type Workloader struct {
	cfg *Config
	db  *sql.DB

	measurement *measurement.Measurement
}

var _ workload.Workloader = &Workloader{}

func NewWorkloader(db *sql.DB, cfg *Config) workload.Workloader {
	return &Workloader{
		db:  db,
		cfg: cfg,
		measurement: measurement.NewMeasurement(func(m *measurement.Measurement) {
			m.MinLatency = 100 * time.Microsecond
			m.MaxLatency = 20 * time.Minute
			m.SigFigs = 3
		}),
	}
}

func (w *Workloader) Name() string {
	return "rawsql"
}

func (w *Workloader) InitThread(ctx context.Context, threadID int) context.Context {
	s := &rawsqlState{
		queryIdx: threadID,
		TpcState: workload.NewTpcState(ctx, w.db),
	}

	ctx = context.WithValue(ctx, stateKey, s)
	return ctx
}

func (w *Workloader) getState(ctx context.Context) *rawsqlState {
	s := ctx.Value(stateKey).(*rawsqlState)
	return s
}

func (w *Workloader) updateState(ctx context.Context) {
	s := w.getState(ctx)
	s.queryIdx++
}

func (w *Workloader) CleanupThread(ctx context.Context, threadID int) {
	s := w.getState(ctx)
	s.Conn.Close()
}

func (w *Workloader) Run(ctx context.Context, threadID int) error {
	s := w.getState(ctx)
	defer w.updateState(ctx)

	if err := s.Conn.PingContext(ctx); err != nil {
		time.Sleep(w.cfg.RefreshWait) // I feel it silly to sleep, but don't come up with better idea
		if err := s.RefreshConn(ctx); err != nil {
			return err
		}
	}

	queryName := w.cfg.QueryNames[s.queryIdx%len(w.cfg.QueryNames)]
	query := w.cfg.Queries[queryName]
	if w.cfg.ExecExplainAnalyze {
		query = "explain analyze\n" + query
	}

	start := time.Now()
	rows, err := s.Conn.QueryContext(ctx, query)
	w.measurement.Measure(queryName, time.Since(start), err)
	if err != nil {
		return fmt.Errorf("execute query %s failed %v", queryName, err)
	}
	if w.cfg.ExecExplainAnalyze {
		table, err := util.RenderExplainAnalyze(rows)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "explain analyze result of query %s:\n%s\n", queryName, table)
		return nil
	}

	defer rows.Close()
	return nil
}

func outputMeasurement(prefix string, opMeasurement map[string]*measurement.Histogram) {
	keys := make([]string, len(opMeasurement))
	var i = 0
	for k := range opMeasurement {
		keys[i] = k
		i += 1
	}
	sort.Strings(keys)

	for _, op := range keys {
		hist := opMeasurement[op]
		if !hist.Empty() {
			fmt.Printf("%s%s: %.2fs\n", prefix, strings.ToUpper(op), float64(hist.GetInfo().Avg)/1000)
		}
	}
}

func (w *Workloader) OutputStats(ifSummaryReport bool) {
	w.measurement.Output(ifSummaryReport, outputMeasurement)
}

func (w *Workloader) DBName() string {
	return w.cfg.DBName
}

func (w *Workloader) Prepare(ctx context.Context, threadID int) error {
	// how to prepare data is undecided
	panic("not implemented") // TODO: Implement
}

func (w *Workloader) CheckPrepare(ctx context.Context, threadID int) error {
	panic("not implemented") // TODO: Implement
}

func (w *Workloader) Cleanup(ctx context.Context, threadID int) error {
	panic("not implemented") // TODO: Implement
}

func (w *Workloader) Check(ctx context.Context, threadID int) error {
	panic("not implemented") // TODO: Implement
}
