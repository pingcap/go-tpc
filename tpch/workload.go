package tpch

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/go-tpc/pkg/measurement"
	replayer "github.com/pingcap/go-tpc/pkg/plan-replayer"
	"github.com/pingcap/go-tpc/pkg/util"
	"github.com/pingcap/go-tpc/pkg/workload"
	"github.com/pingcap/go-tpc/tpch/dbgen"
)

type contextKey string

const stateKey = contextKey("tpch")

// analyzeConfig is the configuration for analyze after data loaded
type analyzeConfig struct {
	Enable                     bool
	BuildStatsConcurrency      int
	DistsqlScanConcurrency     int
	IndexSerialScanConcurrency int
}

// Config is the configuration for tpch workload
type Config struct {
	Driver             string
	DBName             string
	RawQueries         string
	QueryNames         []string
	ScaleFactor        int
	EnableOutputCheck  bool
	TiFlashReplica     int
	AnalyzeTable       analyzeConfig
	ExecExplainAnalyze bool
	PrepareThreads     int

	PlanReplayerConfig replayer.PlanReplayerConfig
	EnablePlanReplayer bool

	// for prepare command only
	OutputType string
	OutputDir  string

	// output style
	OutputStyle string
}

type tpchState struct {
	*workload.TpcState
	queryIdx int
}

// Workloader is TPCH workload
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
			m.MinLatency = 100 * time.Millisecond
			m.MaxLatency = 20 * time.Minute
			m.SigFigs = 3
		}),
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
func (w *Workloader) Name() string {
	return "tpch"
}

// InitThread inits thread
func (w *Workloader) InitThread(ctx context.Context, threadID int) context.Context {
	s := &tpchState{
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
	if err := w.createTables(ctx); err != nil {
		return err
	}
	var sqlLoader map[dbgen.Table]dbgen.Loader
	if w.cfg.OutputType == "csv" {
		if _, err := os.Stat(w.cfg.OutputDir); err != nil {
			if os.IsNotExist(err) {
				if err := os.Mkdir(w.cfg.OutputDir, os.ModePerm); err != nil {
					return err
				}
			} else {
				return err
			}
		}
		sqlLoader = map[dbgen.Table]dbgen.Loader{
			dbgen.TOrder:  dbgen.NewOrderLoader(util.CreateFile(path.Join(w.cfg.OutputDir, fmt.Sprintf("%s.orders.csv", w.DBName())))),
			dbgen.TLine:   dbgen.NewLineItemLoader(util.CreateFile(path.Join(w.cfg.OutputDir, fmt.Sprintf("%s.lineitem.csv", w.DBName())))),
			dbgen.TPart:   dbgen.NewPartLoader(util.CreateFile(path.Join(w.cfg.OutputDir, fmt.Sprintf("%s.part.csv", w.DBName())))),
			dbgen.TPsupp:  dbgen.NewPartSuppLoader(util.CreateFile(path.Join(w.cfg.OutputDir, fmt.Sprintf("%s.partsupp.csv", w.DBName())))),
			dbgen.TSupp:   dbgen.NewSuppLoader(util.CreateFile(path.Join(w.cfg.OutputDir, fmt.Sprintf("%s.supplier.csv", w.DBName())))),
			dbgen.TCust:   dbgen.NewCustLoader(util.CreateFile(path.Join(w.cfg.OutputDir, fmt.Sprintf("%s.customer.csv", w.DBName())))),
			dbgen.TNation: dbgen.NewNationLoader(util.CreateFile(path.Join(w.cfg.OutputDir, fmt.Sprintf("%s.nation.csv", w.DBName())))),
			dbgen.TRegion: dbgen.NewRegionLoader(util.CreateFile(path.Join(w.cfg.OutputDir, fmt.Sprintf("%s.region.csv", w.DBName())))),
		}
	} else {
		sqlLoader = map[dbgen.Table]dbgen.Loader{
			dbgen.TOrder:  NewOrderLoader(ctx, w.db, w.cfg.PrepareThreads),
			dbgen.TLine:   NewLineItemLoader(ctx, w.db, w.cfg.PrepareThreads),
			dbgen.TPart:   NewPartLoader(ctx, w.db, w.cfg.PrepareThreads),
			dbgen.TPsupp:  NewPartSuppLoader(ctx, w.db, w.cfg.PrepareThreads),
			dbgen.TSupp:   NewSuppLoader(ctx, w.db, w.cfg.PrepareThreads),
			dbgen.TCust:   NewCustLoader(ctx, w.db, w.cfg.PrepareThreads),
			dbgen.TNation: NewNationLoader(ctx, w.db, w.cfg.PrepareThreads),
			dbgen.TRegion: NewRegionLoader(ctx, w.db, w.cfg.PrepareThreads),
		}
	}

	dbgen.InitDbGen(int64(w.cfg.ScaleFactor))
	if err := dbgen.DbGen(sqlLoader, []dbgen.Table{dbgen.TNation, dbgen.TRegion, dbgen.TCust, dbgen.TSupp, dbgen.TPartPsupp, dbgen.TOrderLine}); err != nil {
		return err
	}

	// After data loaded, analyze tables to speed up queries.
	if w.cfg.AnalyzeTable.Enable {
		if err := w.analyzeTables(ctx, w.cfg.AnalyzeTable); err != nil {
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
		if err := s.RefreshConn(ctx); err != nil {
			return err
		}
	}

	queryName := w.cfg.QueryNames[s.queryIdx%len(w.cfg.QueryNames)]
	query := query(w.cfg.Driver, queryName)

	// only for driver == mysql and EnablePlanReplayer == true
	if w.cfg.EnablePlanReplayer && w.cfg.Driver == "mysql" {
		err := w.dumpPlanReplayer(ctx, s, query, queryName)
		if err != nil {
			return err
		}
	}

	if w.cfg.ExecExplainAnalyze {
		query = strings.Replace(query, "/*PLACEHOLDER*/", "explain analyze", 1)
	}
	start := time.Now()
	rows, err := s.Conn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("execute query %s failed %v", query, err)
	}
	defer rows.Close()
	w.measurement.Measure(queryName, time.Now().Sub(start), err)

	if err != nil {
		return fmt.Errorf("execute %s failed %v", queryName, err)
	}

	if w.cfg.ExecExplainAnalyze {
		table, err := util.RenderExplainAnalyze(rows)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "explain analyze result of query %s:\n%s\n", queryName, table)
		return nil
	}
	if err := w.scanQueryResult(queryName, rows); err != nil {
		return fmt.Errorf("check %s failed %v", queryName, err)
	}
	return nil
}

// Cleanup cleans up workloader
func (w *Workloader) Cleanup(ctx context.Context, threadID int) error {
	if threadID != 0 {
		return nil
	}
	return w.dropTable(ctx)
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
	w.measurement.Output(ifSummaryReport, w.cfg.OutputStyle, outputRtMeasurement)
}

// DBName returns the name of test db.
func (w *Workloader) DBName() string {
	return w.cfg.DBName
}

func (w *Workloader) dumpPlanReplayer(ctx context.Context, s *tpchState, query, queryName string) error {
	query = strings.Replace(query, "/*PLACEHOLDER*/", "plan replayer dump explain", 1)
	return w.PlanReplayerRunner.Dump(ctx, s.Conn, query, queryName)
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
