package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	// mysql package
	_ "github.com/go-sql-driver/mysql"
)

var (
	dbName     string
	host       string
	port       int
	user       string
	password   string
	threads    int
	driver     string
	totalTime  time.Duration
	totalCount int

	globalDB  *sql.DB
	globalCtx context.Context
)

func closeDB() {
	if globalDB != nil {
		globalDB.Close()
	}
	globalDB = nil
}

func openDB() {
	// TODO: support other drivers
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, dbName)
	var err error
	globalDB, err = sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}

	globalDB.SetMaxIdleConns(threads + 1)
}

func main() {
	var rootCmd = &cobra.Command{
		Use:   "dbbench",
		Short: "Benchmark database with different workloads",
	}

	rootCmd.PersistentFlags().StringVarP(&dbName, "db", "D", "test", "Database name")
	rootCmd.PersistentFlags().StringVarP(&host, "host", "H", "127.0.0.1", "Database host")
	rootCmd.PersistentFlags().StringVarP(&user, "user", "U", "root", "Database user")
	rootCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "Database password")
	rootCmd.PersistentFlags().IntVarP(&port, "port", "P", 4000, "Database port")
	rootCmd.PersistentFlags().IntVarP(&threads, "threads", "T", 16, "Thread concurrency")
	rootCmd.PersistentFlags().StringVarP(&driver, "driver", "d", "", "Database driver: mysql")
	rootCmd.PersistentFlags().DurationVar(&totalTime, "time", 10*time.Minute, "Total execution time")
	rootCmd.PersistentFlags().IntVar(&totalCount, "count", 1000000, "Total execution count")

	cobra.EnablePrefixMatching = true

	registerTpcc(rootCmd)

	var cancel context.CancelFunc
	globalCtx, cancel = context.WithTimeout(context.Background(), totalTime)

	rootCmd.Execute()

	cancel()
}
