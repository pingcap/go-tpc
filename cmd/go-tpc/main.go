package main

import (
	"context"
	"crypto/sha1"
	"database/sql"
	sqldrv "database/sql/driver"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pingcap/go-tpc/pkg/dtable"
	"github.com/pingcap/go-tpc/pkg/util"
	"github.com/spf13/cobra"
	_ "go.uber.org/automaxprocs"
	// mysql package
	"github.com/go-sql-driver/mysql"
	// pg
	"github.com/lib/pq"
)

var (
	dbName         string
	hosts          []string
	ports          []int
	statusPort     int
	user           string
	password       string
	threads        int
	acThreads      int
	driver         string
	totalTime      time.Duration
	totalCount     int
	dropData       bool
	ignoreError    bool
	outputInterval time.Duration
	isolationLevel int
	silence        bool
	pprofAddr      string
	metricsAddr    string
	maxProcs       int
	connParams     string
	outputStyle    string
	targets        []string

	globalDB  *sql.DB
	globalCtx context.Context

	baseId     string
	privateKey string
)

const (
	createDBDDL  = "CREATE DATABASE "
	mysqlDriver  = "mysql"
	pgDriver     = "postgres"
	dtableDriver = "dtable"
)

type MuxDriver struct {
	cursor    uint64
	instances []string
	internal  sqldrv.Driver
}

func (drv *MuxDriver) Open(name string) (sqldrv.Conn, error) {
	k := atomic.AddUint64(&drv.cursor, 1)
	return drv.internal.Open(drv.instances[int(k)%len(drv.instances)])
}

func makeTargets(hosts []string, ports []int) []string {
	targets := make([]string, 0, len(hosts)*len(ports))
	for _, host := range hosts {
		for _, port := range ports {
			targets = append(targets, host+":"+strconv.Itoa(port))
		}
	}
	return targets
}

func newDB(targets []string, driver string, user string, password string, dbName string, connParams string) (*sql.DB, error) {
	if len(targets) == 0 {
		panic(fmt.Errorf("empty targets"))
	}
	var (
		drv   sqldrv.Driver
		hash  = sha1.New()
		names = make([]string, len(targets))
	)
	hash.Write([]byte(driver))
	hash.Write([]byte(user))
	hash.Write([]byte(password))
	hash.Write([]byte(dbName))
	hash.Write([]byte(connParams))
	for i, addr := range targets {
		hash.Write([]byte(addr))
		switch driver {
		case mysqlDriver:
			// allow multiple statements in one query to allow q15 on the TPC-H
			dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?multiStatements=true", user, password, addr, dbName)
			if len(connParams) > 0 {
				dsn = dsn + "&" + connParams
			}
			names[i] = dsn
			drv = &mysql.MySQLDriver{}
		case pgDriver:
			dsn := fmt.Sprintf("postgres://%s:%s@%s/%s", user, password, addr, dbName)
			if len(connParams) > 0 {
				dsn = dsn + "?" + connParams
			}
			names[i] = dsn
			drv = &pq.Driver{}
		case dtableDriver:
			dsn := fmt.Sprintf("http://%s?baseId=%s&privateKey=%s", addr, baseId, privateKey)
			names[i] = dsn
			drv = &dtable.Driver{}
		default:
			panic(fmt.Errorf("unknown driver: %q", driver))
		}
	}

	if len(names) == 1 {
		return sql.Open(driver, names[0])
	}
	drvName := driver + "+" + hex.EncodeToString(hash.Sum(nil))
	for _, n := range sql.Drivers() {
		if n == drvName {
			return sql.Open(drvName, "")
		}
	}
	sql.Register(drvName, &MuxDriver{instances: names, internal: drv})
	return sql.Open(drvName, "")
}

func closeDB() {
	if globalDB != nil {
		globalDB.Close()
	}
	globalDB = nil
}

func openDB() {
	var (
		tmpDB *sql.DB
		err   error
	)
	globalDB, err = newDB(targets, driver, user, password, dbName, connParams)
	if err != nil {
		panic(err)
	}
	if err := globalDB.Ping(); err != nil {
		if isDBNotExist(err) {
			tmpDB, _ = newDB(targets, driver, user, password, "", connParams)
			defer tmpDB.Close()
			if _, err := tmpDB.Exec(createDBDDL + dbName); err != nil {
				panic(fmt.Errorf("failed to create database, err %v\n", err))
			}
		} else {
			globalDB = nil
		}
	} else {
		globalDB.SetMaxIdleConns(threads + acThreads + 1)
	}
}

func isDBNotExist(err error) bool {
	if err == nil {
		return false
	}
	switch driver {
	case mysqlDriver:
		return strings.Contains(err.Error(), "Unknown database")
	case pgDriver:
		msg := err.Error()
		return strings.HasPrefix(msg, "pq: database") && strings.HasSuffix(msg, "does not exist")
	}
	return false
}

func main() {
	var rootCmd = &cobra.Command{
		Use:   "go-tpc",
		Short: "Benchmark database with different workloads",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if len(targets) == 0 {
				targets = makeTargets(hosts, ports)
			}
		},
	}
	rootCmd.PersistentFlags().IntVar(&maxProcs, "max-procs", 0, "runtime.GOMAXPROCS")
	rootCmd.PersistentFlags().StringVar(&pprofAddr, "pprof", "", "Address of pprof endpoint")
	rootCmd.PersistentFlags().StringVar(&metricsAddr, "metrics-addr", "", "Address of metrics endpoint")
	rootCmd.PersistentFlags().StringVarP(&dbName, "db", "D", "test", "Database name")
	rootCmd.PersistentFlags().StringSliceVarP(&hosts, "host", "H", []string{"127.0.0.1"}, "Database host")
	rootCmd.PersistentFlags().StringVarP(&user, "user", "U", "root", "Database user")
	rootCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "Database password")
	rootCmd.PersistentFlags().IntSliceVarP(&ports, "port", "P", []int{4000}, "Database port")
	rootCmd.PersistentFlags().IntVarP(&statusPort, "statusPort", "S", 10080, "Database status port")
	rootCmd.PersistentFlags().IntVarP(&threads, "threads", "T", 1, "Thread concurrency")
	rootCmd.PersistentFlags().IntVarP(&acThreads, "acThreads", "t", 1, "OLAP client concurrency, only for CH-benCHmark")
	rootCmd.PersistentFlags().StringVarP(&driver, "driver", "d", mysqlDriver, "Database driver: mysql, postgres")
	rootCmd.PersistentFlags().DurationVar(&totalTime, "time", 1<<63-1, "Total execution time")
	rootCmd.PersistentFlags().IntVar(&totalCount, "count", 0, "Total execution count, 0 means infinite")
	rootCmd.PersistentFlags().BoolVar(&dropData, "dropdata", false, "Cleanup data before prepare")
	rootCmd.PersistentFlags().BoolVar(&ignoreError, "ignore-error", false, "Ignore error when running workload")
	rootCmd.PersistentFlags().BoolVar(&silence, "silence", false, "Don't print error when running workload")
	rootCmd.PersistentFlags().DurationVar(&outputInterval, "interval", 10*time.Second, "Output interval time")
	rootCmd.PersistentFlags().IntVar(&isolationLevel, "isolation", 0, `Isolation Level 0: Default, 1: ReadUncommitted,
2: ReadCommitted, 3: WriteCommitted, 4: RepeatableRead,
5: Snapshot, 6: Serializable, 7: Linerizable`)
	rootCmd.PersistentFlags().StringVar(&connParams, "conn-params", "", "session variables, e.g. for TiDB --conn-params tidb_isolation_read_engines='tiflash', For PostgreSQL: --conn-params sslmode=disable")
	rootCmd.PersistentFlags().StringVar(&outputStyle, "output", util.OutputStylePlain, "output style, valid values can be { plain | table | json }")
	rootCmd.PersistentFlags().StringSliceVar(&targets, "targets", nil, "Target database addresses")
	rootCmd.PersistentFlags().StringVar(&baseId, "base-id", "", "Dtable base Id")
	rootCmd.PersistentFlags().StringVar(&privateKey, "private-key", "", "Dtable private key")
	rootCmd.PersistentFlags().MarkHidden("targets")

	cobra.EnablePrefixMatching = true

	registerVersionInfo(rootCmd)
	registerTpcc(rootCmd)
	registerTpch(rootCmd)
	registerCHBenchmark(rootCmd)
	registerRawsql(rootCmd)

	var cancel context.CancelFunc
	globalCtx, cancel = context.WithCancel(context.Background())

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
