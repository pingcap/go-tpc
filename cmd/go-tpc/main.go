package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	// mysql package
	_ "github.com/go-sql-driver/mysql"
)

var (
	dbName         string
	host           string
	port           int
	user           string
	password       string
	threads        int
	driver         string
	totalTime      time.Duration
	totalCount     int
	dropData       bool
	ignoreError    bool
	outputInterval time.Duration
	isolationLevel int
	silence        bool

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
		Use:   "go-tpc",
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
	rootCmd.PersistentFlags().BoolVar(&dropData, "dropdata", false, "Cleanup data before prepare")
	rootCmd.PersistentFlags().BoolVar(&ignoreError, "ignore-error", false, "Ignore error when running workload")
	rootCmd.PersistentFlags().BoolVar(&silence, "silence", false, "Don't print error when running workload")
	rootCmd.PersistentFlags().DurationVar(&outputInterval, "interval", 10*time.Second, "Output interval time")
	rootCmd.PersistentFlags().IntVar(&isolationLevel, "isolation", 0, `Isolation Level 0: Default, 1: ReadUncommitted, 
2: ReadCommitted, 3: WriteCommitted, 4: RepeatableRead, 
5: Snapshot, 6: Serializable, 7: Linerizable`)

	cobra.EnablePrefixMatching = true

	registerTpcc(rootCmd)

	var cancel context.CancelFunc
	globalCtx, cancel = context.WithTimeout(context.Background(), totalTime)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	closeDone := make(chan struct{}, 1)
	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		cancel()

		select {
		case <-sc:
			// send signal again, return directly
			fmt.Printf("\nGot signal [%v] again to exit.\n", sig)
			os.Exit(1)
		case <-time.After(10 * time.Second):
			fmt.Print("\nWait 10s for closed, force exit\n")
			os.Exit(1)
		case <-closeDone:
			return
		}
	}()

	rootCmd.Execute()

	cancel()
}
