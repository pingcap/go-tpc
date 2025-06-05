package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/go-tpc/pkg/util"
	"github.com/pingcap/go-tpc/tpch"
	"github.com/spf13/cobra"
)

var tpchConfig tpch.Config

var queryTuningVars = []struct {
	name  string
	value string
}{
	// For optimal join order, esp. for q9.
	{"tidb_default_string_match_selectivity", "0.1"},
	// For optimal join order for all queries.
	{"tidb_opt_join_reorder_threshold", "60"},
	// For optimal join type between broadcast and hash partition join.
	{"tidb_prefer_broadcast_join_by_exchange_data_size", "ON"},
}

// isSysVarSupported determines if a system variable is supported in given TiDB version
// TODO: Every known sys var should have a minimal supported version and be checked individually. For now we just assume all sys vars are supported since 7.1.0.
func isSysVarSupported(ver *semver.Version, _ string) bool {
	return ver.Compare(semver.Version{Major: 7, Minor: 1, Patch: 0}) >= 0
}

// isInvertedIndexSupported checks if inverted index is supported in given TiDB version
func isInvertedIndexSupported(ver semver.Version) bool {
	return ver.Compare(semver.Version{Major: 9, Minor: 0, Patch: 0, PreRelease: "beta.1"}) > 0
}

func executeTpch(action string) {
	openDB()
	defer closeDB()

	if globalDB == nil {
		util.StdErrLogger.Printf("cannot connect to the database")
		os.Exit(1)
	}
	if maxProcs != 0 {
		runtime.GOMAXPROCS(maxProcs)
	}

	isTiDB := false
	var TiDBSemVer *semver.Version
	if driver == mysqlDriver {
		serverVer, err := getServerVersion(globalDB)
		if err != nil {
			panic(fmt.Errorf("get server version failed: %v", err))
		}
		fmt.Printf("Server version: %s\n", serverVer)
		TiDBSemVer, isTiDB = util.NewTiDBSemVersion(serverVer)
	}

	if action == "run" && tpchConfig.EnableQueryTuning {
		if isTiDB {
			fmt.Printf("Enabling query tuning for TiDB version %s.\n", TiDBSemVer.String())
			if err := setTiDBQueryTuningVars(globalDB, TiDBSemVer); err != nil {
				panic(fmt.Errorf("set session variables failed: %v", err))
			}
		} else {
			fmt.Printf("Query tuning is enabled(by default) but server version doesn't appear to be TiDB, skipping tuning.\n")
		}
	}

	if tpchConfig.AddInvertedIndex {
		if !isTiDB || !isInvertedIndexSupported(*TiDBSemVer) || tpchConfig.TiFlashReplica == 0 {
			fmt.Printf("Inverted index is only supported when TiDB version is > 9.0.0-beta.1 and TiFlash replica > 0.\n")
			tpchConfig.AddInvertedIndex = false
		} else {
			fmt.Printf("Enabling inverted index to accelerate queries.\n")
			tpchConfig.AddInvertedIndex = true
		}
	}

	tpchConfig.PlanReplayerConfig.Host = hosts[0]
	tpchConfig.PlanReplayerConfig.StatusPort = statusPort

	tpchConfig.OutputStyle = outputStyle
	tpchConfig.Driver = driver
	tpchConfig.DBName = dbName
	tpchConfig.PrepareThreads = threads
	tpchConfig.QueryNames = strings.Split(tpchConfig.RawQueries, ",")
	w := tpch.NewWorkloader(globalDB, &tpchConfig)
	timeoutCtx, cancel := context.WithTimeout(globalCtx, totalTime)
	defer cancel()

	executeWorkload(timeoutCtx, w, threads, action)
	fmt.Println("Finished")
	w.OutputStats(true)
}

func getServerVersion(db *sql.DB) (string, error) {
	var version string
	err := db.QueryRow("SELECT VERSION()").Scan(&version)
	return version, err
}

func setTiDBQueryTuningVars(db *sql.DB, ver *semver.Version) error {
	for _, v := range queryTuningVars {
		if isSysVarSupported(ver, v.name) {
			if _, err := db.Exec(fmt.Sprintf("SET SESSION %s = %s", v.name, v.value)); err != nil {
				return err
			}
		} else {
			fmt.Printf("Unsupported query tunning var %s for TiDB version %s \n", v.name, ver.String())
		}
	}
	return nil
}

func registerTpch(root *cobra.Command) {
	cmd := &cobra.Command{
		Use: "tpch",
	}

	cmd.PersistentFlags().StringVar(&tpchConfig.RawQueries,
		"queries",
		"q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22",
		"All queries")

	cmd.PersistentFlags().IntVar(&tpchConfig.ScaleFactor,
		"sf",
		1,
		"scale factor")

	cmd.PersistentFlags().BoolVar(&tpchConfig.ExecExplainAnalyze,
		"use-explain",
		false,
		"execute explain analyze")

	cmd.PersistentFlags().BoolVar(&tpchConfig.EnableOutputCheck,
		"check",
		false,
		"Check output data, only when the scale factor equals 1")

	var cmdPrepare = &cobra.Command{
		Use:   "prepare",
		Short: "Prepare data for the workload",
		Run: func(cmd *cobra.Command, args []string) {
			executeTpch("prepare")
		},
	}

	cmdPrepare.PersistentFlags().IntVar(&tpchConfig.TiFlashReplica,
		"tiflash-replica",
		0,
		"Number of tiflash replica")

	cmdPrepare.PersistentFlags().BoolVar(&tpchConfig.AnalyzeTable.Enable,
		"analyze",
		false,
		"After data loaded, analyze table to collect column statistics")
	// https://pingcap.com/docs/stable/reference/performance/statistics/#control-analyze-concurrency
	cmdPrepare.PersistentFlags().IntVar(&tpchConfig.AnalyzeTable.BuildStatsConcurrency,
		"tidb_build_stats_concurrency",
		4,
		"tidb_build_stats_concurrency param for analyze jobs")
	cmdPrepare.PersistentFlags().IntVar(&tpchConfig.AnalyzeTable.DistsqlScanConcurrency,
		"tidb_distsql_scan_concurrency",
		15,
		"tidb_distsql_scan_concurrency param for analyze jobs")
	cmdPrepare.PersistentFlags().IntVar(&tpchConfig.AnalyzeTable.IndexSerialScanConcurrency,
		"tidb_index_serial_scan_concurrency",
		1,
		"tidb_index_serial_scan_concurrency param for analyze jobs")
	cmdPrepare.PersistentFlags().StringVar(&tpchConfig.OutputType,
		"output-type",
		"",
		"Output file type. If empty, then load data to db. Current only support csv")
	cmdPrepare.PersistentFlags().StringVar(&tpchConfig.OutputDir,
		"output-dir",
		"",
		"Output directory for generating file if specified")

	var cmdRun = &cobra.Command{
		Use:   "run",
		Short: "Run workload",
		Run: func(cmd *cobra.Command, args []string) {
			executeTpch("run")
		},
	}

	cmdRun.PersistentFlags().BoolVar(&tpchConfig.EnablePlanReplayer,
		"use-plan-replayer",
		false,
		"Use Plan Replayer to dump stats and variables before running queries")

	cmdRun.PersistentFlags().StringVar(&tpchConfig.PlanReplayerConfig.PlanReplayerDir,
		"plan-replayer-dir",
		"",
		"Dir of Plan Replayer file dumps")

	cmdRun.PersistentFlags().StringVar(&tpchConfig.PlanReplayerConfig.PlanReplayerFileName,
		"plan-replayer-file",
		"",
		"Name of plan Replayer file dumps")

	cmdRun.PersistentFlags().BoolVar(&tpchConfig.EnableQueryTuning,
		"enable-query-tuning",
		true,
		"Tune queries by setting some session variables known effective for tpch")
	cmdRun.PersistentFlags().BoolVar(&tpchConfig.AddInvertedIndex,
		"add-inverted-index",
		true,
		"Add some inverted index to accelerate queries")

	var cmdCleanup = &cobra.Command{
		Use:   "cleanup",
		Short: "Cleanup data for the workload",
		Run: func(cmd *cobra.Command, args []string) {
			executeTpch("cleanup")
		},
	}

	cmd.AddCommand(cmdRun, cmdPrepare, cmdCleanup)

	root.AddCommand(cmd)
}
