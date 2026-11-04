package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/pingcap/go-tpc/pkg/measurement"
	"github.com/pingcap/go-tpc/pkg/workload"
	"github.com/pingcap/go-tpc/tpcc"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

var tpccConfig tpcc.Config

func startHTTPServer(name, addr string, handler http.Handler) {
	if host, _, err := net.SplitHostPort(addr); err == nil && host != "localhost" && !net.ParseIP(host).IsLoopback() {
		fmt.Fprintf(os.Stderr, "Warning: %s endpoint at %s has no authentication; bind to 127.0.0.1 to restrict access to this machine.\n", name, addr)
	}
	go func() {
		s := http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       60 * time.Second,
			// Leave WriteTimeout unset to allow long CPU profiles and traces.
		}
		if err := s.ListenAndServe(); err != nil {
			fmt.Printf("Failed to listen %s: %v\n", name, err)
			os.Exit(1)
		}
	}()
}

func executeTpcc(action string) {
	if pprofAddr != "" {
		mux := http.NewServeMux()
		// Use a private mux to exclude Cmdline, which exposes argv passwords.
		// Index still serves named runtime profiles such as heap and goroutine.
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		startHTTPServer("pprof", pprofAddr, mux)
	}
	if metricsAddr != "" {
		startHTTPServer("metrics", metricsAddr, promhttp.Handler())
	}
	if maxProcs != 0 {
		runtime.GOMAXPROCS(maxProcs)
	}

	openDB()
	defer closeDB()

	tpccConfig.OutputStyle = outputStyle
	tpccConfig.Driver = driver
	tpccConfig.DBName = dbName
	tpccConfig.Threads = threads
	tpccConfig.Isolation = isolationLevel
	var (
		w   workload.Workloader
		err error
	)
	switch tpccConfig.OutputType {
	case "csv", "CSV":
		if tpccConfig.OutputDir == "" {
			fmt.Printf("Output Directory cannot be empty when generating files")
			os.Exit(1)
		}
		w, err = tpcc.NewCSVWorkloader(globalDB, &tpccConfig)
	default:
		// Set a reasonable connection max lifetime when auto-refresh is enabled
		// This ensures connections are actually closed and not just returned to pool
		if tpccConfig.ConnRefreshInterval > 0 {
			globalDB.SetConnMaxLifetime(tpccConfig.ConnRefreshInterval)
			fmt.Printf("Auto-setting connection max lifetime to %v (refresh interval)\n", tpccConfig.ConnRefreshInterval)
		}

		w, err = tpcc.NewWorkloader(globalDB, &tpccConfig)
	}

	if err != nil {
		fmt.Printf("Failed to init work loader: %v\n", err)
		os.Exit(1)
	}

	timeoutCtx, cancel := context.WithTimeout(globalCtx, totalTime)
	defer cancel()

	executeWorkload(timeoutCtx, w, threads, action)

	fmt.Println("Finished")
	w.OutputStats(true)
}

func registerTpcc(root *cobra.Command) {
	cmd := &cobra.Command{
		Use: "tpcc",
	}

	cmd.PersistentFlags().IntVar(&tpccConfig.Parts, "parts", 1, "Number to partition warehouses")
	cmd.PersistentFlags().IntVar(&tpccConfig.PartitionType, "partition-type", 1, "Partition type (1 - HASH, 2 - RANGE, 3 - LIST (like HASH), 4 - LIST (like RANGE)")
	cmd.PersistentFlags().IntVar(&tpccConfig.Warehouses, "warehouses", 10, "Number of warehouses")
	cmd.PersistentFlags().BoolVar(&tpccConfig.CheckAll, "check-all", false, "Run all consistency checks")
	var cmdPrepare = &cobra.Command{
		Use:   "prepare",
		Short: "Prepare data for TPCC",
		Run: func(cmd *cobra.Command, _ []string) {
			executeTpcc("prepare")
		},
	}
	cmdPrepare.PersistentFlags().BoolVar(&tpccConfig.NoCheck, "no-check", false, "TPCC prepare check, default false")
	cmdPrepare.PersistentFlags().BoolVar(&tpccConfig.UseFK, "use-fk", false, "TPCC using foreign key, default false")
	cmdPrepare.PersistentFlags().BoolVar(&tpccConfig.UseClusteredIndex, "use-clustered-index", true, "TPCC use clustered index, default true")
	cmdPrepare.PersistentFlags().StringVar(&tpccConfig.OutputType, "output-type", "", "Output file type."+
		" If empty, then load data to db. Current only support csv")
	cmdPrepare.PersistentFlags().StringVar(&tpccConfig.OutputDir, "output-dir", "", "Output directory for generating file if specified")
	cmdPrepare.PersistentFlags().StringVar(&tpccConfig.SpecifiedTables, "tables", "", "Specified tables for "+
		"generating file, separated by ','. Valid only if output is set. If this flag is not set, generate all tables by default")
	cmdPrepare.PersistentFlags().IntVar(&tpccConfig.PrepareRetryCount, "retry-count", 50, "Retry count when errors occur")
	cmdPrepare.PersistentFlags().DurationVar(&tpccConfig.PrepareRetryInterval, "retry-interval", 10*time.Second, "The interval for each retry")

	var cmdRun = &cobra.Command{
		Use:   "run",
		Short: "Run workload",
		Run: func(cmd *cobra.Command, _ []string) {
			executeTpcc("run")
		},
	}
	cmdRun.PersistentFlags().BoolVar(&tpccConfig.Wait, "wait", false, "including keying & thinking time described on TPC-C Standard Specification")
	cmdRun.PersistentFlags().DurationVar(&tpccConfig.MaxMeasureLatency, "max-measure-latency", measurement.DefaultMaxLatency, "max measure latency in millisecond")
	cmdRun.PersistentFlags().IntSliceVar(&tpccConfig.Weight, "weight", []int{45, 43, 4, 4, 4}, "Weight for NewOrder, Payment, OrderStatus, Delivery, StockLevel")
	cmdRun.Flags().DurationVar(&tpccConfig.ConnRefreshInterval, "conn-refresh-interval", 0, "automatically refresh database connections at specified intervals to balance traffic across new replicas (0 = disabled, e.g., 10s)")

	var cmdCleanup = &cobra.Command{
		Use:   "cleanup",
		Short: "Cleanup data for the workload",
		Run: func(cmd *cobra.Command, _ []string) {
			executeTpcc("cleanup")
		},
	}

	var cmdCheck = &cobra.Command{
		Use:   "check",
		Short: "Check data consistency for the workload",
		Run: func(cmd *cobra.Command, _ []string) {
			executeTpcc("check")
		},
	}

	cmd.AddCommand(cmdRun, cmdPrepare, cmdCleanup, cmdCheck)

	root.AddCommand(cmd)
}
