# Go TPC

A toolbox to benchmark workloads in [TPC](http://www.tpc.org/)

## Install

```bash
make
```

## TPC-C

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
./bin/go-tpc tpcc --warehouses 4 prepare --csv.output data
# Specified tables when generating csv files
./bin/go-tpc tpcc --warehouses 4 prepare --csv.output data --csv.table history --csv.table orders

# Start pprof
./bin/go-tpc tpcc --warehouses 4 prepare --csv.output data --pprof :10111
```

## TPC-H

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
