# Go TPC

A toolbox to benchmark workloads in [TPC](http://www.tpc.org/) for TiDB and almost MySQL compatible databases, and PostgreSQL compatible database, such as PostgreSQL / CockroachDB / AlloyDB / Yugabyte.

## Install

You can use one of the three approaches

### Install using script(recommend)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/pingcap/go-tpc/master/install.sh | sh
```

And then open a new terminal to try `go-tpc`

### Download binary

You can download the pre-built binary [here](https://github.com/pingcap/go-tpc/releases) and then gunzip it

### Build from source

```bash
git clone https://github.com/pingcap/go-tpc.git
cd go-tpc
make build
```

Then you can find the `go-tpc` binary file in the `./bin` directory.

## Usage

If you have `go-tpc` in your PATH, the command below you should replace `./bin/go-tpc` with `go-tpc`

By default, go-tpc uses `root::@tcp(127.0.0.1:4000)/test` as the default dsn address, you can override it by setting below flags:

```bash
  -D, --db string           Database name (default "test")
  -H, --host string         Database host (default "127.0.0.1")
  -p, --password string     Database password
  -P, --port int            Database port (default 4000)
  -U, --user string         Database user (default "root")

```

> **Note:**
>
> When exporting csv files to a directory, `go-tpc` will also create the necessary tables for further data input if
> the provided database address is accessible.

For example:

```bash
./bin/go-tpc -H 127.0.0.1 -P 3306 -D tpcc ...
```

### TPC-C

#### Prepare

##### TiDB & MySQL

```bash
# Create 4 warehouses with 4 threads
./bin/go-tpc tpcc --warehouses 4 prepare -T 4
```

##### PostgreSQL & CockroachDB & AlloyDB & Yugabyte


```
./bin/go-tpc tpcc prepare -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
```

#### Run

##### TiDB & MySQL

```bash
# Run TPCC workloads, you can just run or add --wait option to including wait times
./bin/go-tpc tpcc --warehouses 4 run -T 4
# Run TPCC including wait times(keying & thinking time) on every transactions
./bin/go-tpc tpcc --warehouses 4 run -T 4 --wait
```

##### PostgreSQL & CockroachDB & AlloyDB & Yugabyte

```
./bin/go-tpc tpcc run -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
```

#### Check

```bash
# Check consistency. you can check after prepare or after run
./bin/go-tpc tpcc --warehouses 4 check
```

#### Clean up

```bash
# Cleanup
./bin/go-tpc tpcc --warehouses 4 cleanup
```

#### Other usages

```bash
# Generate csv files (split to 100 files each table)
./bin/go-tpc tpcc --warehouses 4 prepare -T 100 --output-type csv --output-dir data
# Specified tables when generating csv files
./bin/go-tpc tpcc --warehouses 4 prepare -T 100 --output-type csv --output-dir data --tables history,orders
# Start pprof
./bin/go-tpc tpcc --warehouses 4 prepare --output-type csv --output-dir data --pprof :10111
```

If you want to import tpcc data into TiDB, please refer to [import-to-tidb](docs/import-to-tidb.md).

### TPC-H

#### Prepare

##### TiDB & MySQL

```bash
# Prepare data with scale factor 1
./bin/go-tpc tpch --sf=1 prepare
# Prepare data with scale factor 1, create tiflash replica, and analyze table after data loaded
./bin/go-tpc tpch --sf 1 --analyze --tiflash prepare
```

##### PostgreSQL & CockroachDB & AlloyDB & Yugabyte

```
./bin/go-tpc tpch prepare -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
```

#### Run
##### TiDB & MySQL

```bash
# Run TPCH workloads with result checking
./bin/go-tpc tpch --sf=1 --check=true run
# Run TPCH workloads without result checking
./bin/go-tpc tpch --sf=1 run
```

##### PostgreSQL & CockroachDB & AlloyDB & Yugabyte

```
./bin/go-tpc tpch run -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
```
#### Clean up

```bash
# Cleanup
./bin/go-tpc tpch cleanup
```

### CH-benCHmark

#### Prepare

1. First please refer to the above instruction(`go-tpc tpcc --warehouses $warehouses prepare`) to prepare the TP part schema and populate data

2. Then uses `go-tpc ch prepare` to prepare the AP part schema and data

A detail example to run CH workload on TiDB can be refered to [TiDB Doc](https://docs.pingcap.com/tidb/dev/benchmark-tidb-using-ch)

##### TiDB & MySQL
```bash
# Prepare TP data
./bin/go-tpc tpcc --warehouses 10 prepare -T 4 -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
# Prepare AP data, create tiflash replica, and analyze table after data loaded
./bin/go-tpc ch --analyze --tiflash prepare -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
```
##### PostgreSQL & CockroachDB & AlloyDB & Yugabyte

``` bash
# Prepare TP data
./bin/go-tpc tpcc prepare -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable -T 4
# Prepare AP data
./bin/go-tpc ch prepare -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
```

#### Run

##### TiDB & MySQL
```bash
./bin/go-tpc ch --warehouses $warehouses -T $tpWorkers -t $apWorkers --time $measurement-time run
```
##### PostgreSQL & CockroachDB & AlloyDB & Yugabyte

```
./bin/go-tpc ch run -d postgres -U myuser -p '12345678' -D test -H 127.0.0.1 -P 5432 --conn-params sslmode=disable
```

### Raw SQL
`rawsql` command is used to execute sql from given sql files.

#### Run
```bash
./bin/go-tpc rawsql run --query-files $path-to-query-files
```
