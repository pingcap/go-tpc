package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/pingcap/go-tpc/ch"
	"github.com/pingcap/go-tpc/pkg/workload"
	"github.com/pingcap/go-tpc/tpcc"
)

var chConfig ch.Config
var (
	apConnParams string
	apHost       string
	apPort       int
)

func registerCHBenchmark(root *cobra.Command) {
	cmd := &cobra.Command{
		Use: "ch",
	}
	cmd.PersistentFlags().IntVar(&tpccConfig.Parts, "parts", 1, "Number to partition warehouses")
	cmd.PersistentFlags().IntVar(&tpccConfig.Warehouses, "warehouses", 10, "Number of warehouses")
	cmd.PersistentFlags().BoolVar(&tpccConfig.CheckAll, "check-all", false, "Run all consistency checks")
	cmd.PersistentFlags().StringVar(&chConfig.RawQueries,
		"queries",
		"q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22",
		"All queries")
	cmd.PersistentFlags().DurationVar(&chConfig.RefreshConnWait, "refresh-conn-wait", 5*time.Second, "duration to wait before refreshing sql connection")

	var cmdPrepare = &cobra.Command{
		Use:   "prepare",
		Short: "Prepare data for the workload",
		Run: func(cmd *cobra.Command, args []string) {
			executeCH("prepare", nil)
		},
	}
	cmdPrepare.PersistentFlags().BoolVar(&chConfig.CreateTiFlashReplica,
		"tiflash",
		false,
		"Create tiflash replica")

	cmdPrepare.PersistentFlags().BoolVar(&chConfig.AnalyzeTable.Enable,
		"analyze",
		false,
		"After data loaded, analyze table to collect column statistics")
	// https://pingcap.com/docs/stable/reference/performance/statistics/#control-analyze-concurrency
	cmdPrepare.PersistentFlags().IntVar(&chConfig.AnalyzeTable.BuildStatsConcurrency,
		"tidb_build_stats_concurrency",
		4,
		"tidb_build_stats_concurrency param for analyze jobs")
	cmdPrepare.PersistentFlags().IntVar(&chConfig.AnalyzeTable.DistsqlScanConcurrency,
		"tidb_distsql_scan_concurrency",
		15,
		"tidb_distsql_scan_concurrency param for analyze jobs")
	cmdPrepare.PersistentFlags().IntVar(&chConfig.AnalyzeTable.IndexSerialScanConcurrency,
		"tidb_index_serial_scan_concurrency",
		1,
		"tidb_index_serial_scan_concurrency param for analyze jobs")

	var cmdRun = &cobra.Command{
		Use:   "run",
		Short: "Run workload",
		Run: func(cmd *cobra.Command, _ []string) {
			executeCH("run", func() string {
				origConnParams, origHost, origPort := connParams, host, port
				defer func() {
					connParams, host, port = origConnParams, origHost, origPort
				}()
				if len(apConnParams) > 0 {
					connParams = apConnParams
				}
				if len(apHost) > 0 {
					host = apHost
				}
				if apPort > 0 {
					port = apPort
				}
				return buildDSN(false)
			})
		},
	}
	cmdRun.PersistentFlags().BoolVar(&chConfig.EnablePlanReplayer,
		"use-plan-replayer",
		false,
		"Use Plan Replayer to dump stats and variables before running queries")

	cmdRun.PersistentFlags().StringVar(&chConfig.PlanReplayerConfig.PlanReplayerDir,
		"plan-replayer-dir",
		"",
		"Dir of Plan Replayer file dumps")

	cmdRun.PersistentFlags().StringVar(&chConfig.PlanReplayerConfig.PlanReplayerFileName,
		"plan-replayer-file",
		"",
		"Name of plan Replayer file dumps")

	cmdRun.PersistentFlags().IntSliceVar(&tpccConfig.Weight, "weight", []int{45, 43, 4, 4, 4}, "Weight for NewOrder, Payment, OrderStatus, Delivery, StockLevel")
	cmdRun.Flags().StringVar(&apConnParams, "ap-conn-params", "", "Connection parameters for analytical processing")
	cmdRun.Flags().StringVar(&apHost, "ap-host", "", "Database host for analytical processing")
	cmdRun.Flags().IntVar(&apPort, "ap-port", 0, "Database port for analytical processing")
	cmd.AddCommand(cmdRun, cmdPrepare)
	root.AddCommand(cmd)
}

func executeCH(action string, buildDSNForAP func() string) {
	runtime.GOMAXPROCS(maxProcs)

	openDB()
	defer closeDB()

	tpccConfig.OutputStyle = outputStyle
	tpccConfig.Driver = driver
	tpccConfig.DBName = dbName
	tpccConfig.Threads = threads
	tpccConfig.Isolation = isolationLevel
	chConfig.OutputStyle = outputStyle
	chConfig.Driver = driver
	chConfig.DBName = dbName
	chConfig.QueryNames = strings.Split(chConfig.RawQueries, ",")
	if len(apHost) > 0 {
		chConfig.PlanReplayerConfig.Host = apHost
	} else {
		chConfig.PlanReplayerConfig.Host = host
	}
	chConfig.PlanReplayerConfig.StatusPort = statusPort

	var (
		tp, ap workload.Workloader
		err    error
	)
	tp, err = tpcc.NewWorkloader(globalDB, &tpccConfig)
	if err != nil {
		fmt.Printf("Failed to init tp work loader: %v\n", err)
		os.Exit(1)
	}
	if buildDSNForAP == nil {
		ap = ch.NewWorkloader(globalDB, &chConfig)
	} else {
		db, err := sql.Open(driver, buildDSNForAP())
		if err != nil {
			fmt.Printf("Failed to open db for analytical processing: %v\n", err)
			os.Exit(1)
		}
		db.SetMaxIdleConns(acThreads + 1)
		ap = ch.NewWorkloader(db, &chConfig)
	}
	if err != nil {
		fmt.Printf("Failed to init tp work loader: %v\n", err)
		os.Exit(1)
	}
	timeoutCtx, cancel := context.WithTimeout(globalCtx, totalTime)
	defer cancel()

	if action == "prepare" {
		executeWorkload(timeoutCtx, ap, 1, "prepare")
		return
	}

	type workLoaderSetting struct {
		workLoader workload.Workloader
		threads    int
	}
	var doneWg sync.WaitGroup
	for _, workLoader := range []workLoaderSetting{{workLoader: tp, threads: threads}, {workLoader: ap, threads: acThreads}} {
		doneWg.Add(1)
		go func(workLoader workload.Workloader, threads int) {
			executeWorkload(timeoutCtx, workLoader, threads, "run")
			doneWg.Done()
		}(workLoader.workLoader, workLoader.threads)
	}
	doneWg.Wait()
	fmt.Printf("Finished: %d OLTP workers, %d OLAP workers\n", threads, acThreads)
	for _, workLoader := range []workLoaderSetting{{workLoader: tp, threads: threads}, {workLoader: ap, threads: acThreads}} {
		workLoader.workLoader.OutputStats(true)
	}
}
