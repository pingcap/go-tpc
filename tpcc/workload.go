package tpcc

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/siddontang/go-tpc/pkg/workload"
)

// Config is the configuration for tpcc workload
type Config struct {
	Threads    int
	Tables     int
	Warehouses int
	UseFK      bool
}

// Workloader is TPCC workload
type Workloader struct {
	base workload.BaseWorkloader

	cfg *Config

	createTableWg sync.WaitGroup
}

// NewWorkloader creates the tpc-c workloader
func NewWorkloader(db *sql.DB, cfg *Config) workload.Workloader {
	w := &Workloader{
		base: workload.BaseWorkloader{DB: db},
		cfg:  cfg,
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
	return w.base.InitThread(ctx, threadID)
}

// CleanupThread implements Workloader interface
func (w *Workloader) CleanupThread(ctx context.Context, threadID int) {
	w.base.CleanupThread(ctx, threadID)
}

// Prepare implements Workloader interface
func (w *Workloader) Prepare(ctx context.Context, threadID int) error {
	for i := threadID % w.cfg.Threads; i < w.cfg.Tables; i += w.cfg.Threads {
		if err := w.createTable(ctx, i+1); err != nil {
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

	for i := threadID % w.cfg.Threads; i < w.cfg.Tables; i += w.cfg.Threads {
		// load items
		if err := w.loadItem(ctx, i+1); err != nil {
			return fmt.Errorf("load item%d faield %v", i+1, err)
		}
	}

	wareshouses := w.cfg.Tables * w.cfg.Warehouses
	for i := threadID % w.cfg.Threads; i < wareshouses; i += w.cfg.Threads {
		tableID := i/w.cfg.Warehouses + 1
		warehouse := i%w.cfg.Warehouses + 1

		// load warehouse
		if err := w.loadWarhouse(ctx, tableID, warehouse); err != nil {
			return fmt.Errorf("load warehouse%d in %d failed %v", tableID, warehouse, err)
		}
		// load stock
		if err := w.loadStock(ctx, tableID, warehouse); err != nil {
			return fmt.Errorf("load stock%d in %d failed %v", tableID, warehouse, err)
		}

		// load distict
		if err := w.loadDistrict(ctx, tableID, warehouse); err != nil {
			return fmt.Errorf("load district%d with %d failed %v", tableID, warehouse, err)

		}
	}

	// districts := w.cfg.Tables * w.cfg.Scales * 10
	// for i := threadID % w.cfg.Threads; i < districts; i += w.cfg.Threads {
	// 	tableID := i / (w.cfg.Scales * 10)
	// 	warehouse := (i / 10) % w.cfg.Scales
	// 	district := i % 10

	// 	// load customer
	// 	// load hisotry
	// 	// load order
	// 	// loader order-line
	// 	// load new-order
	// }

	return nil
}

// Run implements Workloader interface
func (w *Workloader) Run(ctx context.Context, threadID int) error {
	return nil
}

// Cleanup implements Workloader interface
func (w *Workloader) Cleanup(ctx context.Context, threadID int) error {
	for i := threadID % w.cfg.Threads; i < w.cfg.Tables; i += w.cfg.Threads {
		if err := w.dropTable(ctx, i+1); err != nil {
			return err
		}
	}
	return nil
}
