package tpcc

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/pingcap/go-tpc/pkg/measurement"
	"github.com/pingcap/go-tpc/pkg/util"
	"github.com/pingcap/go-tpc/pkg/workload"
)

type contextKey string

const stateKey = contextKey("tpcc")

var tables = []string{tableItem, tableCustomer, tableDistrict, tableHistory,
	tableNewOrder, tableOrderLine, tableOrders, tableStock, tableWareHouse}

type txn struct {
	name   string
	action func(ctx context.Context, threadID int) error
	weight int
	// keyingTime time.Duration
	// thinkingTime time.Duration
}

type tpccState struct {
	*workload.TpcState
	index int
	decks []int
	files map[string]*os.File

	newOrderStmts    map[string]*sql.Stmt
	orderStatusStmts map[string]*sql.Stmt
	deliveryStmts    map[string]*sql.Stmt
	stockLevelStmt   map[string]*sql.Stmt
	paymentStmts     map[string]*sql.Stmt
}

// Config is the configuration for tpcc workload
type Config struct {
	DBName     string
	Threads    int
	Parts      int
	Warehouses int
	UseFK      bool
	Isolation  int
	CheckAll   bool
	OutputDir  string
	Tables     []string
}

// Workloader is TPCC workload
type Workloader struct {
	db *sql.DB

	cfg *Config

	createTableWg sync.WaitGroup
	initLoadTime  string

	// tables is a set to keep the specified tables for generating csv file.
	tables map[string]bool

	txns []txn
}

// NewWorkloader creates the tpc-c workloader
func NewWorkloader(db *sql.DB, cfg *Config) (workload.Workloader, error) {
	if cfg.Parts > cfg.Warehouses {
		panic(fmt.Errorf("number warehouses %d must >= partition %d", cfg.Warehouses, cfg.Parts))
	}

	w := &Workloader{
		db:           db,
		cfg:          cfg,
		initLoadTime: time.Now().Format(timeFormat),
		tables:       make(map[string]bool),
	}

	w.txns = []txn{
		{name: "new_order", action: w.runNewOrder, weight: 45},
		{name: "payment", action: w.runPayment, weight: 43},
		{name: "order_status", action: w.runOrderStatus, weight: 4},
		{name: "delivery", action: w.runDelivery, weight: 4},
		{name: "stock_level", action: w.runStockLevel, weight: 4},
	}

	var val bool
	if len(cfg.Tables) == 0 {
		val = true
	}
	for _, table := range tables {
		w.tables[table] = val
	}

	if w.cfg.OutputDir != "" {
		if _, err := os.Stat(w.cfg.OutputDir); err != nil {
			if os.IsNotExist(err) {
				if err := os.Mkdir(w.cfg.OutputDir, os.ModePerm); err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		}

		for _, t := range cfg.Tables {
			if _, ok := w.tables[t]; !ok {
				return nil, fmt.Errorf("\nTable %s is not supported.\nSupported tables: item, customer, district, "+
					"orders, new_order, order_line, history, warehouse, stock.", t)
			}
			w.tables[t] = true
		}

		if !w.tables[tableOrders] && w.tables[tableOrderLine] {
			return nil, fmt.Errorf("\nTable orders must be specified if you want to generate table order_line.")
		}
	} else {
		w.createTableWg.Add(cfg.Threads)
	}

	return w, nil
}

// Name implements Workloader interface
func (w *Workloader) Name() string {
	return "tpcc"
}

// InitThread implements Workloader interface
func (w *Workloader) InitThread(ctx context.Context, threadID int) context.Context {
	s := &tpccState{
		TpcState: workload.NewTpcState(ctx, w.db),
		index:    0,
		decks:    make([]int, 0, 23),
	}

	for index, txn := range w.txns {
		for i := 0; i < txn.weight; i++ {
			s.decks = append(s.decks, index)
		}
	}

	if w.DataGen() {
		s.files = make(map[string]*os.File)
		for k, v := range w.tables {
			if v {
				s.files[k] = util.CreateFile(path.Join(w.cfg.OutputDir, fmt.Sprintf("%s.%s.%d.csv", w.DBName(), k, threadID)))
			}
		}
	}

	s.index = len(s.decks) - 1

	ctx = context.WithValue(ctx, stateKey, s)

	return ctx
}

// CleanupThread implements Workloader interface
func (w *Workloader) CleanupThread(ctx context.Context, threadID int) {
	s := w.getState(ctx)
	closeStmts(s.newOrderStmts)
	closeStmts(s.paymentStmts)
	closeStmts(s.deliveryStmts)
	closeStmts(s.stockLevelStmt)
	closeStmts(s.orderStatusStmts)
	// TODO: close stmts for delivery, order status, and stock level
	if s.Conn != nil {
		s.Conn.Close()
	}
	for k, _ := range s.files {
		s.files[k].Close()
	}
}

// Prepare implements Workloader interface
func (w *Workloader) Prepare(ctx context.Context, threadID int) error {
	if !w.DataGen() {
		if threadID == 0 {
			if err := w.createTable(ctx); err != nil {
				return err
			}
		}
		w.createTableWg.Done()
		w.createTableWg.Wait()
	}

	// - 100,1000 rows in the ITEM table
	// - 1 row in the WAREHOUSE table for each configured warehouse
	// 	For each row in the WAREHOUSE table
	//	+ 100,000 rows in the STOCK table
	//	+ 10 rows in the DISTRICT table
	//		For each row in the DISTRICT table
	//		* 3,000 rows in the CUSTOMER table
	//			For each row in the CUSTOMER table
	//			- 1 row in the HISTORY table
	//		* 3,000 rows in the ORDER table
	//			For each row in the ORDER table
	//			- A number of rows in the ORDER-LINE table equal to O_OL_CNT,
	//			  generated according to the rules for input data generation
	//			  of the New-Order transaction
	//  	* 900 rows in the NEW-ORDER table corresponding to the last 900 rows
	//		  in the ORDER table for that district

	if threadID == 0 {
		// load items
		if err := w.loadItem(ctx); err != nil {
			return fmt.Errorf("load item faield %v", err)
		}
	}

	for i := threadID % w.cfg.Threads; i < w.cfg.Warehouses; i += w.cfg.Threads {
		warehouse := i%w.cfg.Warehouses + 1

		// load warehouse
		if err := w.loadWarehouse(ctx, warehouse); err != nil {
			return fmt.Errorf("load warehouse in %d failed %v", warehouse, err)
		}
		// load stock
		if err := w.loadStock(ctx, warehouse); err != nil {
			return fmt.Errorf("load stock at warehouse %d failed %v", warehouse, err)
		}

		// load district
		if err := w.loadDistrict(ctx, warehouse); err != nil {
			return fmt.Errorf("load district at wareshouse %d failed %v", warehouse, err)

		}
	}

	districts := w.cfg.Warehouses * districtPerWarehouse
	var err error
	for i := threadID % w.cfg.Threads; i < districts; i += w.cfg.Threads {
		warehouse := (i/districtPerWarehouse)%w.cfg.Warehouses + 1
		district := i%districtPerWarehouse + 1

		// load customer
		if err = w.loadCustomer(ctx, warehouse, district); err != nil {
			return fmt.Errorf("load customer at warehouse %d district %d failed %v", warehouse, district, err)
		}
		// load history
		if err = w.loadHistory(ctx, warehouse, district); err != nil {
			return fmt.Errorf("load history at warehouse %d district %d failed %v", warehouse, district, err)
		}
		// load orders
		var olCnts []int
		if olCnts, err = w.loadOrder(ctx, warehouse, district); err != nil {
			return fmt.Errorf("load orders at warehouse %d district %d failed %v", warehouse, district, err)
		}
		// loader new-order
		if err = w.loadNewOrder(ctx, warehouse, district); err != nil {
			return fmt.Errorf("load new_order at warehouse %d district %d failed %v", warehouse, district, err)
		}
		// load order-line
		if err = w.loadOrderLine(ctx, warehouse, district, olCnts); err != nil {
			return fmt.Errorf("load order_line at warehouse %d district %d failed %v", warehouse, district, err)
		}
	}

	return nil
}

func (w *Workloader) getState(ctx context.Context) *tpccState {
	s := ctx.Value(stateKey).(*tpccState)
	return s
}

// Run implements Workloader interface
func (w *Workloader) Run(ctx context.Context, threadID int) error {
	s := w.getState(ctx)

	if s.newOrderStmts == nil {
		s.newOrderStmts = map[string]*sql.Stmt{
			newOrderSelectCustomer: prepareStmt(ctx, s.Conn, newOrderSelectCustomer),
			newOrderSelectDistrict: prepareStmt(ctx, s.Conn, newOrderSelectDistrict),
			newOrderUpdateDistrict: prepareStmt(ctx, s.Conn, newOrderUpdateDistrict),
			newOrderInsertOrder:    prepareStmt(ctx, s.Conn, newOrderInsertOrder),
			newOrderInsertNewOrder: prepareStmt(ctx, s.Conn, newOrderInsertNewOrder),
			// batch select items
			// batch select stock for update
			newOrderUpdateStock: prepareStmt(ctx, s.Conn, newOrderUpdateStock),
			// batch insert order_line
		}
		for i := 5; i <= 15; i++ {
			s.newOrderStmts[newOrderSelectItemSQLs[i]] = prepareStmt(ctx, s.Conn, newOrderSelectItemSQLs[i])
			s.newOrderStmts[newOrderSelectStockSQLs[i]] = prepareStmt(ctx, s.Conn, newOrderSelectStockSQLs[i])
			s.newOrderStmts[newOrderInsertOrderLineSQLs[i]] = prepareStmt(ctx, s.Conn, newOrderInsertOrderLineSQLs[i])
		}

		s.paymentStmts = map[string]*sql.Stmt{
			paymentUpdateWarehouse:          prepareStmt(ctx, s.Conn, paymentUpdateWarehouse),
			paymentSelectWarehouse:          prepareStmt(ctx, s.Conn, paymentSelectWarehouse),
			paymentUpdateDistrict:           prepareStmt(ctx, s.Conn, paymentUpdateDistrict),
			paymentSelectDistrict:           prepareStmt(ctx, s.Conn, paymentSelectDistrict),
			paymentSelectCustomerListByLast: prepareStmt(ctx, s.Conn, paymentSelectCustomerListByLast),
			paymentSelectCustomerForUpdate:  prepareStmt(ctx, s.Conn, paymentSelectCustomerForUpdate),
			paymentSelectCustomerData:       prepareStmt(ctx, s.Conn, paymentSelectCustomerData),
			paymentUpdateCustomerWithData:   prepareStmt(ctx, s.Conn, paymentUpdateCustomerWithData),
			paymentUpdateCustomer:           prepareStmt(ctx, s.Conn, paymentUpdateCustomer),
			paymentInsertHistory:            prepareStmt(ctx, s.Conn, paymentInsertHistory),
		}

		s.orderStatusStmts = map[string]*sql.Stmt{
			orderStatusSelectCustomerCntByLast: prepareStmt(ctx, s.Conn, orderStatusSelectCustomerCntByLast),
			orderStatusSelectCustomerByLast:    prepareStmt(ctx, s.Conn, orderStatusSelectCustomerByLast),
			orderStatusSelectCustomerByID:      prepareStmt(ctx, s.Conn, orderStatusSelectCustomerByID),
			orderStatusSelectLatestOrder:       prepareStmt(ctx, s.Conn, orderStatusSelectLatestOrder),
			orderStatusSelectOrderLine:         prepareStmt(ctx, s.Conn, orderStatusSelectOrderLine),
		}
		s.deliveryStmts = map[string]*sql.Stmt{
			deliverySelectNewOrder:  prepareStmt(ctx, s.Conn, deliverySelectNewOrder),
			deliveryDeleteNewOrder:  prepareStmt(ctx, s.Conn, deliveryDeleteNewOrder),
			deliveryUpdateOrder:     prepareStmt(ctx, s.Conn, deliveryUpdateOrder),
			deliverySelectOrders:    prepareStmt(ctx, s.Conn, deliverySelectOrders),
			deliveryUpdateOrderLine: prepareStmt(ctx, s.Conn, deliveryUpdateOrderLine),
			deliverySelectSumAmount: prepareStmt(ctx, s.Conn, deliverySelectSumAmount),
			deliveryUpdateCustomer:  prepareStmt(ctx, s.Conn, deliveryUpdateCustomer),
		}
		s.stockLevelStmt = map[string]*sql.Stmt{
			stockLevelSelectDistrict: prepareStmt(ctx, s.Conn, stockLevelSelectDistrict),
			stockLevelCount:          prepareStmt(ctx, s.Conn, stockLevelCount),
		}
	}

	// refer 5.2.4.2
	if s.index == len(s.decks) {
		s.index = 0
		s.R.Shuffle(len(s.decks), func(i, j int) {
			s.decks[i], s.decks[j] = s.decks[j], s.decks[i]
		})
	}

	txnIndex := s.decks[s.R.Intn(len(s.decks))]
	txn := w.txns[txnIndex]

	start := time.Now()
	err := txn.action(ctx, threadID)

	measurement.Measure(txn.name, time.Now().Sub(start), err)

	// TODO: add check
	return err
}

// Cleanup implements Workloader interface
func (w *Workloader) Cleanup(ctx context.Context, threadID int) error {
	if threadID == 0 {
		if err := w.dropTable(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (w *Workloader) beginTx(ctx context.Context) (*sql.Tx, error) {
	s := w.getState(ctx)
	tx, err := s.Conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.IsolationLevel(w.cfg.Isolation),
	})
	return tx, err
}

func prepareStmts(ctx context.Context, conn *sql.Conn, queries []string) []*sql.Stmt {
	stmts := make([]*sql.Stmt, len(queries))
	for i, query := range queries {
		if len(query) == 0 {
			continue
		}
		stmts[i] = prepareStmt(ctx, conn, query)
	}

	return stmts
}

func prepareStmt(ctx context.Context, conn *sql.Conn, query string) *sql.Stmt {
	stmt, err := conn.PrepareContext(ctx, query)
	if err != nil {
		panic(err)
	}
	return stmt
}

func closeStmts(stmts map[string]*sql.Stmt) {
	for _, stmt := range stmts {
		if stmt == nil {
			continue
		}
		stmt.Close()
	}
}

// DataGen returns a bool to represent whether to generate csv data or load data to db.
func (w *Workloader) DataGen() bool {
	return w.cfg.OutputDir != ""
}

// DBName returns the name of test db.
func (w *Workloader) DBName() string {
	return w.cfg.DBName
}
