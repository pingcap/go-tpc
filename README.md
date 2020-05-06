# Go TPC

A toolbox to benchmark workloads in [TPC](http://www.tpc.org/)

## Install

### Download binary

You can download the pre-built binary [here](https://github.com/pingcap/go-tpc/releases)

### Build from source

```bash
git clone https://github.com/pingcap/go-tpc.git
make build
```

Then you can find the `go-tpc` binary file in the `/bin` directory.

## Usage

By default, go-tpc uses `root::@tcp(127.0.0.1:4000)/test` as the default dsn address, you can override it by setting below flags:

```bash
  -D, --db string           Database name (default "test")
  -H, --host string         Database host (default "127.0.0.1")
  -p, --password string     Database password
  -P, --port int            Database port (default 4000)
  -U, --user string         Database user (default "root")

```

For example:

```bash
./bin/go-tpc -H 127.0.0.1 -P 3306 -D tpcc ...
```

### TPC-C


```bash
# Create 4 warehouses and use 4 partitions by HASH 
./bin/go-tpc tpcc --warehouses 4 --parts 4 prepare
# Run TPCC workloads
./bin/go-tpc tpcc --warehouses 4 run
# Cleanup 
./bin/go-tpc tpcc --warehouses 4 cleanup
# Check consistency 
./bin/go-tpc tpcc --warehouses 4 check
# Generate csv files
./bin/go-tpc tpcc --warehouses 4 prepare --output data
# Specified tables when generating csv files
./bin/go-tpc tpcc --warehouses 4 prepare --output data --tables history,orders
# Start pprof
./bin/go-tpc tpcc --warehouses 4 prepare --output data --pprof :10111
```

If you want to import tpcc data into TiDB, please refer to [import-to-tidb](docs/import-to-tidb.md).

### TPC-H

```bash
# Prepare data with scale factor 1
./bin/go-tpc tpch --sf=1 prepare
# Run TPCH workloads with result checking
./bin/go-tpc tpch --sf=1 --check=true run
# Run TPCH workloads without result checking
./bin/go-tpc tpch --sf=1 run
# Cleanup
./bin/go-tpc tpch cleanup
```
