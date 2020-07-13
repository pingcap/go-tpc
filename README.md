# Go TPC

A toolbox to benchmark workloads in [TPC](http://www.tpc.org/)

## Install

### Download binary

You can download the pre-built binary [here](https://github.com/pingcap/go-tpc/releases)

### Install using script
```bash
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/pingcap/go-tpc/master/install.sh | sh
```

### Build from source

```bash
git clone https://github.com/pingcap/go-tpc.git
make build
```

Then you can find the `go-tpc` binary file in the `./bin` directory.

## Usage

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

```bash
# Create 4 warehouses and use 4 partitions by HASH 
./bin/go-tpc tpcc --warehouses 4 --parts 4 prepare
```

#### Run

```bash
# Run TPCC workloads, you can just run or add --wait option to including wait times
./bin/go-tpc tpcc --warehouses 4 run
# Run TPCC including wait times(keying & thinking time) on every transactions
./bin/go-tpc tpcc --warehouses 4 run --wait
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

```bash
# Prepare data with scale factor 1
./bin/go-tpc tpch --sf=1 prepare
# Prepare data with scale factor 1, create tiflash replica, and analyze table after data loaded
./bin/go-tpc tpch --sf 1 --analyze --tiflash prepare
```

#### Run

```bash
# Run TPCH workloads with result checking
./bin/go-tpc tpch --sf=1 --check=true run
# Run TPCH workloads without result checking
./bin/go-tpc tpch --sf=1 run
```

#### Clean up

```bash
# Cleanup
./bin/go-tpc tpch cleanup
```
