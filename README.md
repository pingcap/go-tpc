# Go TPC

A toolbox to benchmark workloads in [TPC](http://www.tpc.org/)

## Install

```bash
make
```

## TPC-C

```bash
# Create 4 warehouses and use 4 partitions by HASH 
./bin/go-tpc tpcc --warehouses 4 --part 4 prepare
# Run TPCC workloads
./bin/go-tpc tpcc --warehouses 4 run
# Cleanup 
./bin/go-tpc tpcc --warehouses 4 cleanup
```

