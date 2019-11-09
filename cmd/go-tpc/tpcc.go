package main

import (
	"github.com/siddontang/go-tpc/tpcc"
	"github.com/spf13/cobra"
)

var tpccConfig tpcc.Config

func executeTpcc(action string, args []string) {
	openDB()
	defer closeDB()

	tpccConfig.Threads = threads
	tpccConfig.Isolation = isolationLevel
	w := tpcc.NewWorkloader(globalDB, &tpccConfig)

	executeWorkload(globalCtx, w, action)
}

func registerTpcc(root *cobra.Command) {
	cmd := &cobra.Command{
		Use: "tpcc",
	}

	cmd.PersistentFlags().IntVar(&tpccConfig.Parts, "parts", 1, "Number to partition warehouses")
	cmd.PersistentFlags().IntVar(&tpccConfig.Warehouses, "warehouses", 10, "Number of warehouses")

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

	cmd.AddCommand(cmdRun, cmdPrepare, cmdCleanup)

	root.AddCommand(cmd)
}
