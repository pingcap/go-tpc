# How to import tpcc data to TiDB

Currently if you want to import tpcc dataset into the database via `go-tpc`, you have two ways:

1. Using the `go-tpc prepare` to load data to DB directly, this is valid for both `MySQL` and `TiDB`.
2. Output the data into CSV files, then import the CSV files to `TiDB` with `lightning` (TiDB only)

This document will explain how to use the above ways to import data into TiDB. For simplicity, we will
start a small `TiDB` cluster using `tiup playground`.

``` bash
➜  ~ tiup playground
Starting /Users/yeya24/.tiup/components/playground/v0.0.9/playground
Playground Bootstrapping...
Starting /Users/yeya24/.tiup/components/pd/v4.0.0-rc.1/pd-server --name=pd-0 --data-dir=/Users/yeya24/.tiup/data/RyBAqGx/pd-0/data --peer-urls=http://127.0.0.1:2380 --advertise-peer-urls=http://127.0.0.1:2380 --client-urls=http://127.0.0.1:2379 --advertise-client-urls=http://127.0.0.1:2379 --log-file=/Users/yeya24/.tiup/data/RyBAqGx/pd-0/pd.log --initial-cluster=pd-0=http://127.0.0.1:2380
Starting /Users/yeya24/.tiup/components/tidb/v4.0.0-rc.1/tidb-server -P 4000 --store=tikv --host=127.0.0.1 --status=10080 --path=127.0.0.1:2379 --log-file=/Users/yeya24/.tiup/data/RyBAqGx/tidb-0/tidb.log
Starting /Users/yeya24/.tiup/components/tikv/v4.0.0-rc.1/tikv-server --addr=127.0.0.1:20160 --status-addr=127.0.0.1:20180 --pd=http://127.0.0.1:2379 --config=/Users/yeya24/.tiup/data/RyBAqGx/tikv-0/tikv.toml --data-dir=/Users/yeya24/.tiup/data/RyBAqGx/tikv-0/data --log-file=/Users/yeya24/.tiup/data/RyBAqGx/tikv-0/tikv.log
.....
CLUSTER START SUCCESSFULLY, Enjoy it ^-^
To connect TiDB: mysql --host 127.0.0.1 --port 4000 -u root
To view the dashboard: http://127.0.0.1:2379/dashboard
```

## Load data directly

This way is very easy and straightforward. However, it will be slower than the second way because 
it just executes the SQL `Insert` statement at the database.

``` bash
go-tpc tpcc prepare --warehouses 100 -D test -H 127.0.0.1 -P 4000 -T 10
```

Here the `warehouses` flag means the data size, the `-T` flag means how many threads we want to use for loading data.

## Using CSV and lightning

This method includes two steps:

1. Export CSV files
2. Import CSV files to TiDB using lightning

### Export CSV files

``` bash
go-tpc tpcc prepare --warehouses 100 -D test -H 127.0.0.1 -P 4000 -T 16 --output-type csv --output-dir csv/
```

In order to export CSV files, we need to specify two flags here. `--output-type` is what type of file we
want to export, and `--output-dir` is the directory of the exported CSV files.

``` bash
ls csv/
test.customer.0.csv    test.district.14.csv   test.history.6.csv     test.order_line.1.csv  test.orders.15.csv     test.stock.7.csv
test.customer.1.csv    test.district.15.csv   test.history.7.csv     test.order_line.10.csv test.orders.2.csv      test.stock.8.csv
test.customer.10.csv   test.district.2.csv    test.history.8.csv     test.order_line.11.csv test.orders.3.csv      test.stock.9.csv
test.customer.11.csv   test.district.3.csv    test.history.9.csv     test.order_line.12.csv test.orders.4.csv      test.warehouse.0.csv
test.customer.12.csv   test.district.4.csv    test.item.0.csv        test.order_line.13.csv test.orders.5.csv      test.warehouse.1.csv
test.customer.13.csv   test.district.5.csv    test.new_order.0.csv   test.order_line.14.csv test.orders.6.csv      test.warehouse.10.csv
test.customer.14.csv   test.district.6.csv    test.new_order.1.csv   test.order_line.15.csv test.orders.7.csv      test.warehouse.11.csv
test.customer.15.csv   test.district.7.csv    test.new_order.10.csv  test.order_line.2.csv  test.orders.8.csv      test.warehouse.12.csv
test.customer.2.csv    test.district.8.csv    test.new_order.11.csv  test.order_line.3.csv  test.orders.9.csv      test.warehouse.13.csv
test.customer.3.csv    test.district.9.csv    test.new_order.12.csv  test.order_line.4.csv  test.stock.0.csv       test.warehouse.14.csv
test.customer.4.csv    test.history.0.csv     test.new_order.13.csv  test.order_line.5.csv  test.stock.1.csv       test.warehouse.15.csv
test.customer.5.csv    test.history.1.csv     test.new_order.14.csv  test.order_line.6.csv  test.stock.10.csv      test.warehouse.2.csv
test.customer.6.csv    test.history.10.csv    test.new_order.15.csv  test.order_line.7.csv  test.stock.11.csv      test.warehouse.3.csv
test.customer.7.csv    test.history.11.csv    test.new_order.2.csv   test.order_line.8.csv  test.stock.12.csv      test.warehouse.4.csv
test.customer.8.csv    test.history.12.csv    test.new_order.3.csv   test.order_line.9.csv  test.stock.13.csv      test.warehouse.5.csv
test.customer.9.csv    test.history.13.csv    test.new_order.4.csv   test.orders.0.csv      test.stock.14.csv      test.warehouse.6.csv
test.district.0.csv    test.history.14.csv    test.new_order.5.csv   test.orders.1.csv      test.stock.15.csv      test.warehouse.7.csv
test.district.1.csv    test.history.15.csv    test.new_order.6.csv   test.orders.10.csv     test.stock.2.csv       test.warehouse.8.csv
test.district.10.csv   test.history.2.csv     test.new_order.7.csv   test.orders.11.csv     test.stock.3.csv       test.warehouse.9.csv
test.district.11.csv   test.history.3.csv     test.new_order.8.csv   test.orders.12.csv     test.stock.4.csv
test.district.12.csv   test.history.4.csv     test.new_order.9.csv   test.orders.13.csv     test.stock.5.csv
test.district.13.csv   test.history.5.csv     test.order_line.0.csv  test.orders.14.csv     test.stock.6.csv
```

After exporting the files, we can check them in the directory. Here all CSV files conform to the naming scheme <db name>.<table name>.<thread number>.csv

### Import data using lightning

Since `Tiup` doesn't support `lightning` so far, we have to download the binary somewhere or build it from source. 
For simplicity, this document will not include that part, please refer to [lightning doc](https://pingcap.com/docs/stable/reference/tools/tidb-lightning/overview/) for more details.

With the `lightning` binary, then it is easy to import data. We also provide an example [config](./tidb-lightning.toml) for `lightning`. You can just execute the command below:

```bash
lightning -c tidb-lightning.toml
```

Please note that:
1. This example config uses `tidb` as `lightning` backend instead of `importer`. Please update related configs if you are using `importer`.
2. Please change `data-source-dir` field to the CSV directory you set in the previous step.
3. Please update the `tidb` section if you have a different set up.

For the status of the import process, please check `tidb-lightning.log`, if you see the logs below, then it is perfect!

```bash
[2020/05/03 12:51:25.004 -04:00] [INFO] [backend.go:265] ["engine close start"] [engineTag=`test`.`stock`:-1] [engineUUID=5565f8ab-07bc-5dfb-a64c-717945dd3a64]
[2020/05/03 12:51:25.004 -04:00] [INFO] [backend.go:267] ["engine close completed"] [engineTag=`test`.`stock`:-1] [engineUUID=5565f8ab-07bc-5dfb-a64c-717945dd3a64] [takeTime=210ns] []
[2020/05/03 12:51:25.004 -04:00] [INFO] [restore.go:1422] ["import and cleanup engine start"] [engineTag=`test`.`stock`:-1] [engineUUID=5565f8ab-07bc-5dfb-a64c-717945dd3a64]
[2020/05/03 12:51:25.004 -04:00] [INFO] [backend.go:279] ["import start"] [engineTag=`test`.`stock`:-1] [engineUUID=5565f8ab-07bc-5dfb-a64c-717945dd3a64] [retryCnt=0]
[2020/05/03 12:51:25.004 -04:00] [INFO] [backend.go:282] ["import completed"] [engineTag=`test`.`stock`:-1] [engineUUID=5565f8ab-07bc-5dfb-a64c-717945dd3a64] [retryCnt=0] [takeTime=304ns] []
[2020/05/03 12:51:25.004 -04:00] [INFO] [backend.go:294] ["cleanup start"] [engineTag=`test`.`stock`:-1] [engineUUID=5565f8ab-07bc-5dfb-a64c-717945dd3a64]
[2020/05/03 12:51:25.004 -04:00] [INFO] [backend.go:296] ["cleanup completed"] [engineTag=`test`.`stock`:-1] [engineUUID=5565f8ab-07bc-5dfb-a64c-717945dd3a64] [takeTime=189ns] []
[2020/05/03 12:51:25.004 -04:00] [INFO] [restore.go:1429] ["import and cleanup engine completed"] [engineTag=`test`.`stock`:-1] [engineUUID=5565f8ab-07bc-5dfb-a64c-717945dd3a64] [takeTime=54.46µs] []
[2020/05/03 12:51:25.004 -04:00] [INFO] [restore.go:602] ["restore table completed"] [table=`test`.`stock`] [takeTime=29.720372962s] []
[2020/05/03 12:51:25.004 -04:00] [INFO] [restore.go:697] ["restore all tables data completed"] [takeTime=38.919570374s] []
[2020/05/03 12:51:25.004 -04:00] [INFO] [restore.go:475] ["everything imported, stopping periodic actions"]
[2020/05/03 12:51:25.004 -04:00] [INFO] [restore.go:1072] ["skip full compaction"]
[2020/05/03 12:51:25.014 -04:00] [INFO] [restore.go:1241] ["clean checkpoints start"] [keepAfterSuccess=false] [taskID=1588524646072446000]
[2020/05/03 12:51:25.014 -04:00] [INFO] [restore.go:1248] ["clean checkpoints completed"] [keepAfterSuccess=false] [taskID=1588524646072446000] [takeTime=152.037µs] []
[2020/05/03 12:51:25.014 -04:00] [INFO] [restore.go:283] ["the whole procedure completed"] [takeTime=38.936017956s] []
[2020/05/03 12:51:25.014 -04:00] [INFO] [main.go:77] ["tidb lightning exit"]
```
