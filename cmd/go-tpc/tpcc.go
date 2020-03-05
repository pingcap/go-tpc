package main

import (
	"context"
	"fmt"
	"os"

	"github.com/pingcap/go-tpc/tpcc"
	"github.com/spf13/cobra"
)

var tpccConfig tpcc.Config

func executeTpcc(action string, args []string) {
	if tpccConfig.OutputDir == "" {
		openDB()
		defer closeDB()

		if len(tpccConfig.Tables) > 0 {
			fmt.Println("Cannot specify generated tables when csv.output flag not set")
			os.Exit(1)
		}
	}

	tpccConfig.DBName = dbName
	tpccConfig.Threads = threads
	tpccConfig.Isolation = isolationLevel
	w, err := tpcc.NewWorkloader(globalDB, &tpccConfig)
	if err != nil {
		fmt.Printf("Failed to init work loader: %v\n", err)
		os.Exit(1)
	}

	timeoutCtx, cancel := context.WithTimeout(globalCtx, totalTime)
	defer cancel()

	executeWorkload(timeoutCtx, w, action)
}

func registerTpcc(root *cobra.Command) {
	cmd := &cobra.Command{
		Use: "tpcc",
	}

	cmd.PersistentFlags().IntVar(&tpccConfig.Parts, "parts", 1, "Number to partition warehouses")
	cmd.PersistentFlags().IntVar(&tpccConfig.Warehouses, "warehouses", 10, "Number of warehouses")
	cmd.PersistentFlags().BoolVar(&tpccConfig.CheckAll, "check-all", false, "Run all consistency checks")
	cmd.PersistentFlags().StringVar(&tpccConfig.OutputDir, "csv.output", "", "Output directory for generating csv file when preparing data")
	cmd.PersistentFlags().StringArrayVar(&tpccConfig.Tables, "csv.table", []string{}, "Specified tables for " +
		"generating csv file(repeated), valid only if csv.output is set. If this flag is not set, generate all tables by default.")

	var cmdPrepare = &cobra.Command{
		Use:   "prepare",
		Short: "Prepare data for the workload",
		Run: func(cmd *cobra.Command, args []string) {
			executeTpcc("prepare", args)
		},
	}

	var cmdRun = &cobra.Command{
		Use:   "run",
		Short: "Run workload",
		Run: func(cmd *cobra.Command, args []string) {
			executeTpcc("run", args)
		},
	}

	var cmdCleanup = &cobra.Command{
		Use:   "cleanup",
		Short: "Cleanup data for the workload",
		Run: func(cmd *cobra.Command, args []string) {
			executeTpcc("cleanup", args)
		},
	}

	var cmdCheck = &cobra.Command{
		Use:   "check",
		Short: "Check data consistency for the workload",
		Run: func(cmd *cobra.Command, args []string) {
			executeTpcc("check", args)
		},
	}

	cmd.AddCommand(cmdRun, cmdPrepare, cmdCleanup, cmdCheck)

	root.AddCommand(cmd)
}
