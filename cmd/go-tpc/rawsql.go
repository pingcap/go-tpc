package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pingcap/go-tpc/rawsql"
	"github.com/spf13/cobra"
)

var (
	rawsqlConfig    rawsql.Config
	queryFiles      string
	refreshConnWait time.Duration
)

func registerRawsql(root *cobra.Command) {
	cmd := &cobra.Command{
		Use: "rawsql",
	}

	cmdRun := &cobra.Command{
		Use:   "run",
		Short: "Run workload",
		Run: func(cmd *cobra.Command, args []string) {
			if len(queryFiles) == 0 {
				fmt.Fprintln(os.Stderr, "empty query files")
				os.Exit(1)
			}

			execRawsql("run")
		},
	}

	cmdRun.PersistentFlags().BoolVar(&rawsqlConfig.EnablePlanReplayer,
		"use-plan-replayer",
		false,
		"Use Plan Replayer to dump stats and variables before running queries")

	cmdRun.PersistentFlags().StringVar(&rawsqlConfig.PlanReplayerConfig.PlanReplayerDir,
		"plan-replayer-dir",
		"",
		"Dir of Plan Replayer file dumps")

	cmdRun.PersistentFlags().StringVar(&rawsqlConfig.PlanReplayerConfig.PlanReplayerFileName,
		"plan-replayer-file",
		"",
		"Name of plan Replayer file dumps")

	cmdRun.PersistentFlags().StringVar(&queryFiles,
		"query-files",
		"",
		"path of query files")

	cmdRun.PersistentFlags().BoolVar(&rawsqlConfig.ExecExplainAnalyze,
		"use-explain",
		false,
		"execute explain analyze")

	cmdRun.PersistentFlags().DurationVar(&refreshConnWait, "refresh-conn-wait", 5*time.Second, "duration to wait before refreshing sql connection")

	cmd.AddCommand(cmdRun)
	root.AddCommand(cmd)
}

func execRawsql(action string) {
	openDB()
	defer closeDB()

	// if globalDB == nil
	if globalDB == nil {
		fmt.Fprintln(os.Stderr, "cannot connect to the database")
		os.Exit(1)
	}

	rawsqlConfig.OutputStyle = outputStyle
	rawsqlConfig.DBName = dbName
	rawsqlConfig.QueryNames = strings.Split(queryFiles, ",")
	rawsqlConfig.Queries = make(map[string]string, len(rawsqlConfig.QueryNames))
	rawsqlConfig.RefreshWait = refreshConnWait
	rawsqlConfig.PlanReplayerConfig.Host = host
	rawsqlConfig.PlanReplayerConfig.StatusPort = statusPort

	for i, filename := range rawsqlConfig.QueryNames {
		queryData, err := ioutil.ReadFile(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read file: %s, err: %v\n", filename, err)
			os.Exit(1)
		}

		baseName := path.Base(filename)
		queryName := strings.TrimSuffix(baseName, path.Ext(baseName))
		rawsqlConfig.QueryNames[i] = queryName
		rawsqlConfig.Queries[queryName] = string(queryData)
	}

	w := rawsql.NewWorkloader(globalDB, &rawsqlConfig)

	timeoutCtx, cancel := context.WithTimeout(globalCtx, totalTime)
	defer cancel()
	executeWorkload(timeoutCtx, w, threads, action)
	fmt.Println("Finished")
	w.OutputStats(true)
}
