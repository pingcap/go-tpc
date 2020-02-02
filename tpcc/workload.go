package tpcc

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/go-tpc/pkg/measurement"
	"github.com/pingcap/go-tpc/pkg/workload"
)

type contextKey string

const stateKey = contextKey("tpcc")

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

	newOrderStmts []*sql.Stmt
	paymentStmts  []*sql.Stmt
}

// Config is the configuration for tpcc workload
type Config struct {
	Threads    int
	Parts      int
	Warehouses int
	UseFK      bool
	Isolation  int
	CheckAll   bool
}

// Workloader is TPCC workload
type Workloader struct {
	db *sql.DB

	cfg *Config

	createTableWg sync.WaitGroup
	initLoadTime  string

	txns []txn
}

// NewWorkloader creates the tpc-c workloader
func NewWorkloader(db *sql.DB, cfg *Config) workload.Workloader {
	if cfg.Parts > cfg.Warehouses {
		panic(fmt.Errorf("number warehouses %d must >= partition %d", cfg.Warehouses, cfg.Parts))
	}

	w := &Workloader{
		db:           db,
		cfg:          cfg,
		initLoadTime: time.Now().Format(timeFormat),
	}

	w.txns = []txn{
		{name: "new_order", action: w.runNewOrder, weight: 10},
		{name: "payment", action: w.runPayment, weight: 10},
		{name: "order_status", action: w.runOrderStatus, weight: 1},
		{name: "delivery", action: w.runDelivery, weight: 1},
		{name: "stock_level", action: w.runStockLevel, weight: 1},
	}
	w.createTableWg.Add(cfg.Threads)
	return w
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

	s.index = len(s.decks) - 1

	ctx = context.WithValue(ctx, stateKey, s)

	return ctx
}

// CleanupThread implements Workloader interface
func (w *Workloader) CleanupThread(ctx context.Context, threadID int) {
	s := w.getState(ctx)
	closeStmts(s.newOrderStmts)
	closeStmts(s.paymentStmts)
	// TODO: close stmts for delivery, order status, and stock level
	s.Conn.Close()
}

// Prepare implements Workloader interface
func (w *Workloader) Prepare(ctx context.Context, threadID int) error {
	if threadID == 0 {
		if err := w.createTable(ctx); err != nil {
			return err
		}
	}

	w.createTableWg.Done()
	w.createTableWg.Wait()

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
	//			- A number of orws in the ORDER-LINE table equal to O_OL_CNT,
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
		if err := w.loadWarhouse(ctx, warehouse); err != nil {
			return fmt.Errorf("load warehouse in %d failed %v", warehouse, err)
		}
		// load stock
		if err := w.loadStock(ctx, warehouse); err != nil {
			return fmt.Errorf("load stock at warehouse %d failed %v", warehouse, err)
		}

		// load distict
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
		// load hisotry
		if err = w.loadHistory(ctx, warehouse, district); err != nil {
			return fmt.Errorf("load history at warehouse %d district %d failed %v", warehouse, district, err)
		}
		// load order
		var olCnts []int
		if olCnts, err = w.loadOrder(ctx, warehouse, district); err != nil {
			return fmt.Errorf("load order at warehouse %d district %d failed %v", warehouse, district, err)
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
		s.newOrderStmts = prepareStmts(ctx, s.Conn, newOrderQueries)
		s.paymentStmts = prepareStmts(ctx, s.Conn, paymentQueries)
		// TODO: prepare stmts for delivery, order status, and stock level
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
	var err error
	for i, query := range queries {
		if len(query) == 0 {
			continue
		}
		stmts[i], err = conn.PrepareContext(ctx, query)
		if err != nil {
			panic(err)
		}
	}

	return stmts
}

func closeStmts(stmts []*sql.Stmt) {
	for _, stmt := range stmts {
		if stmt == nil {
			continue
		}
		stmt.Close()
	}
}
