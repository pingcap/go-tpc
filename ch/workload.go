package ch

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
	"github.com/pingcap/go-tpc/tpch"
	"github.com/pingcap/go-tpc/tpch/dbgen"
)

type contextKey string

const stateKey = contextKey("ch")

// analyzeConfig is the configuration for analyze after data loaded
type analyzeConfig struct {
	Enable                     bool
	BuildStatsConcurrency      int
	DistsqlScanConcurrency     int
	IndexSerialScanConcurrency int
}

// Config is the configuration for ch workload
type Config struct {
	Driver          string
	DBName          string
	RawQueries      string
	QueryNames      []string
	TiFlashReplica  int
	AnalyzeTable    analyzeConfig
	RefreshConnWait time.Duration

	EnablePlanReplayer bool
	PlanReplayerConfig replayer.PlanReplayerConfig

	// output style
	OutputStyle string
}

type chState struct {
	*workload.TpcState
	queryIdx int
}

// Workloader is CH workload
type Workloader struct {
	db  *sql.DB
	cfg *Config

	// stats
	measurement *measurement.Measurement

	PlanReplayerRunner *replayer.PlanReplayerRunner
}

// NewWorkloader new work loader
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

func (w *Workloader) getState(ctx context.Context) *chState {
	s := ctx.Value(stateKey).(*chState)
	return s
}

func (w *Workloader) updateState(ctx context.Context) {
	s := w.getState(ctx)
	s.queryIdx++
}

// Name return workloader name
func (w *Workloader) Name() string {
	return "ch"
}

// InitThread inits thread
func (w *Workloader) InitThread(ctx context.Context, threadID int) context.Context {
	s := &chState{
		queryIdx: threadID % len(w.cfg.QueryNames),
		TpcState: workload.NewTpcState(ctx, w.db),
	}
	ctx = context.WithValue(ctx, stateKey, s)

	return ctx
}

// CleanupThread cleans up thread
func (w *Workloader) CleanupThread(ctx context.Context, threadID int) {
	s := w.getState(ctx)
	s.Conn.Close()
}

// Prepare prepares data
func (w *Workloader) Prepare(ctx context.Context, threadID int) error {
	if threadID != 0 {
		return nil
	}
	s := w.getState(ctx)

	if err := w.createTables(ctx); err != nil {
		return err
	}
	sqlLoader := map[dbgen.Table]dbgen.Loader{
		dbgen.TSupp:   tpch.NewSuppLoader(ctx, w.db, 1),
		dbgen.TNation: tpch.NewNationLoader(ctx, w.db, 1),
		dbgen.TRegion: tpch.NewRegionLoader(ctx, w.db, 1),
	}
	dbgen.InitDbGen(1)
	if err := dbgen.DbGen(sqlLoader, []dbgen.Table{dbgen.TNation, dbgen.TRegion, dbgen.TSupp}); err != nil {
		return err
	}

	if err := w.prepareView(ctx); err != nil {
		return err
	}

	if w.cfg.TiFlashReplica != 0 {
		if err := w.createTiFlashReplica(ctx, s, w.cfg.TiFlashReplica); err != nil {
			return err
		}
	}
	// After data loaded, analyze tables to speed up queries.
	if w.cfg.AnalyzeTable.Enable {
		if err := w.analyzeTables(ctx, w.cfg.AnalyzeTable); err != nil {
			return err
		}
	}
	return nil
}

func (w *Workloader) prepareView(ctx context.Context) error {
	s := w.getState(ctx)
	fmt.Println("creating view revenue1")
	if _, err := s.Conn.ExecContext(ctx, `
create view revenue1 (supplier_no, total_revenue) as (
    select	mod((s_w_id * s_i_id),10000) as supplier_no,
              sum(ol_amount) as total_revenue
    from	order_line, stock
    where ol_i_id = s_i_id and ol_supply_w_id = s_w_id
      and ol_delivery_d >= '2007-01-02 00:00:00.000000'
    group by mod((s_w_id * s_i_id),10000));
`); err != nil {
		return err
	}
	return nil
}

func (w *Workloader) createTiFlashReplica(ctx context.Context, s *chState, numberOfTiflashReplica int) error {
	for _, tableName := range allTables {
		fmt.Printf("creating tiflash replica for %s\n", tableName)
		replicaSQL := fmt.Sprintf("ALTER TABLE %s SET TIFLASH REPLICA %d", tableName, numberOfTiflashReplica)
		if _, err := s.Conn.ExecContext(ctx, replicaSQL); err != nil {
			return err
		}
	}
	return nil
}

func (w *Workloader) analyzeTables(ctx context.Context, acfg analyzeConfig) error {
	s := w.getState(ctx)
	if w.cfg.Driver == "mysql" {
		for _, tbl := range allTables {
			fmt.Printf("analyzing table %s\n", tbl)
			if _, err := s.Conn.ExecContext(ctx, fmt.Sprintf("SET @@session.tidb_build_stats_concurrency=%d; SET @@session.tidb_distsql_scan_concurrency=%d; SET @@session.tidb_index_serial_scan_concurrency=%d; ANALYZE TABLE %s", acfg.BuildStatsConcurrency, acfg.DistsqlScanConcurrency, acfg.IndexSerialScanConcurrency, tbl)); err != nil {
				return err
			}
			fmt.Printf("analyze table %s done\n", tbl)
		}
	} else if w.cfg.Driver == "postgres" {
		for _, tbl := range allTables {
			fmt.Printf("analyzing %s\n", tbl)
			if _, err := s.Conn.ExecContext(ctx, fmt.Sprintf("ANALYZE %s", tbl)); err != nil {
				return err
			}
			fmt.Printf("analyze %s done\n", tbl)
		}
	}

	return nil
}

// CheckPrepare checks prepare
func (w *Workloader) CheckPrepare(ctx context.Context, threadID int) error {
	return nil
}

// Run runs workload
func (w *Workloader) Run(ctx context.Context, threadID int) error {
	s := w.getState(ctx)
	defer w.updateState(ctx)

	if err := s.Conn.PingContext(ctx); err != nil {
		time.Sleep(w.cfg.RefreshConnWait) // I feel it silly to sleep, but don't come up with better idea
		if err := s.RefreshConn(ctx); err != nil {
			return err
		}
	}

	queryName := w.cfg.QueryNames[s.queryIdx%len(w.cfg.QueryNames)]
	query := queries[queryName]

	// only for driver == mysql and EnablePlanReplayer == true
	if w.cfg.EnablePlanReplayer && w.cfg.Driver == "mysql" {
		w.dumpPlanReplayer(ctx, s, query, queryName)
	}

	start := time.Now()
	rows, err := s.Conn.QueryContext(ctx, query)
	w.measurement.Measure(queryName, time.Now().Sub(start), err)
	if err != nil {
		return fmt.Errorf("execute query %s failed %v", queryName, err)
	}
	defer rows.Close()
	return nil
}

// Cleanup cleans up workloader
func (w *Workloader) Cleanup(ctx context.Context, threadID int) error {
	return nil
}

// Check checks data
func (w *Workloader) Check(ctx context.Context, threadID int) error {
	return nil
}

func outputRtMeasurement(outputStyle string, prefix string, opMeasurement map[string]*measurement.Histogram) {
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
			line := []string{prefix, strings.ToUpper(op)}
			line = append(line, chSummary(hist)...)
			lines = append(lines, line)
		}
	}
	switch outputStyle {
	case util.OutputStylePlain:
		util.RenderString("%s%-6s - %s\n", []string{"Prefix", "Operation", "Count", "Sum(ms)", "Avg(ms)"}, lines)
	case util.OutputStyleTable:
		util.RenderTable([]string{"Prefix", "Operation", "Count", "Sum(ms)", "Avg(ms)"}, lines)
	case util.OutputStyleJson:
		util.RenderJson([]string{"Prefix", "Operation", "Count", "Sum(ms)", "Avg(ms)"}, lines)

	}
}

func chSummary(h *measurement.Histogram) []string {
	res := h.GetInfo()

	return []string{
		util.IntToString(res.Count),
		util.FloatToOneString(res.Sum),
		util.FloatToOneString(res.Avg),
	}
}

func (w *Workloader) OutputStats(ifSummaryReport bool) {
	w.measurement.Output(ifSummaryReport, w.cfg.OutputStyle, outputRtMeasurement)
	if ifSummaryReport {
		var count int64
		var elapsed float64
		for _, m := range w.measurement.OpSumMeasurement {
			if !m.Empty() {
				r := m.GetInfo()
				count += r.Count
				elapsed += r.Elapsed
			}
		}
		if elapsed != 0 {
			fmt.Printf("QphH: %.1f\n", 3600/elapsed*float64(count))
		}
	}
}

// DBName returns the name of test db.
func (w *Workloader) DBName() string {
	return w.cfg.DBName
}

func (w *Workloader) dumpPlanReplayer(ctx context.Context, s *chState, query, queryName string) {
	query = strings.Replace(query, "/*PLACEHOLDER*/", "plan replayer dump explain", 1)
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
