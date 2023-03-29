package rawsql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/go-tpc/pkg/measurement"
	replayer "github.com/pingcap/go-tpc/pkg/plan-replayer"
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

	// output style
	OutputStyle        string
	EnablePlanReplayer bool
	PlanReplayerConfig replayer.PlanReplayerConfig
}

type rawsqlState struct {
	queryIdx int
	*workload.TpcState
}

type Workloader struct {
	cfg *Config
	db  *sql.DB

	measurement *measurement.Measurement

	PlanReplayerRunner *replayer.PlanReplayerRunner
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

	if w.cfg.EnablePlanReplayer {
		w.dumpPlanReplayer(ctx, s, query, queryName)
	}

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
		util.StdErrLogger.Printf("explain analyze result of query %s:\n%s\n", queryName, table)
		return nil
	}

	defer rows.Close()
	return nil
}

func outputMeasurement(outputStyle string, prefix string, opMeasurement map[string]*measurement.Histogram) {
	keys := make([]string, len(opMeasurement))
	var i = 0
	for k := range opMeasurement {
		keys[i] = k
		i += 1
	}
	sort.Strings(keys)

	lines := [][]string{}
	for _, op := range keys {
		hist := opMeasurement[op]
		if !hist.Empty() {
			lines = append(lines, []string{prefix, strings.ToUpper(op), util.FloatToTwoString(float64(hist.GetInfo().Avg)/1000) + "s"})
		}
	}

	switch outputStyle {
	case util.OutputStylePlain:
		util.RenderString("%s%s: %s\n", nil, lines)
	case util.OutputStyleTable:
		util.RenderTable([]string{"Prefix", "Operation", "Avg(s)"}, lines)
	case util.OutputStyleJson:
		util.RenderJson([]string{"Prefix", "Operation", "Avg(s)"}, lines)
	}
}

func (w *Workloader) OutputStats(ifSummaryReport bool) {
	w.measurement.Output(ifSummaryReport, w.cfg.OutputStyle, outputMeasurement)
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

func (w *Workloader) dumpPlanReplayer(ctx context.Context, s *rawsqlState, query, queryName string) {
	query = "plan replayer dump explain " + query
	err := w.PlanReplayerRunner.Dump(ctx, s.Conn, query, queryName)
	if err != nil {
		fmt.Printf("dump query %s plan replayer failed %v", queryName, err)
	}
}

func (w *Workloader) IsPlanReplayerDumpEnabled() bool {
	return w.cfg.EnablePlanReplayer
}

func (w *Workloader) PreparePlanReplayerDump() error {
	w.cfg.PlanReplayerConfig.WorkloadName = w.Name()
	if w.PlanReplayerRunner == nil {
		w.PlanReplayerRunner = &replayer.PlanReplayerRunner{
			Config: w.cfg.PlanReplayerConfig,
		}
	}
	return w.PlanReplayerRunner.Prepare()
}

func (w *Workloader) FinishPlanReplayerDump() error {
	return w.PlanReplayerRunner.Finish()
}

func (w *Workloader) Exec(sql string) error {
	return nil
}
