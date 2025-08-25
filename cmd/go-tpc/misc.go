package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/go-tpc/pkg/workload"
)

func checkPrepare(ctx context.Context, w workload.Workloader) {
	// skip preparation check in csv case
	if w.Name() == "tpcc-csv" {
		fmt.Println("Skip preparing checking. Please load CSV data into database and check later.")
		return
	}
	if w.Name() == "tpcc" && tpccConfig.NoCheck {
		return
	}

	var wg sync.WaitGroup
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go func(index int) {
			defer wg.Done()

			ctx = w.InitThread(ctx, index)
			defer w.CleanupThread(ctx, index)

			if err := w.CheckPrepare(ctx, index); err != nil {
				fmt.Printf("check prepare failed, err %v\n", err)
				return
			}
		}(i)
	}
	wg.Wait()
}

func execute(timeoutCtx context.Context, w workload.Workloader, action string, threads, index int) error {
	count := totalCount / threads

	// For prepare, cleanup and check operations, use background context to avoid timeout constraints
	// Only run phases should be limited by timeout
	var ctx context.Context
	if action == "prepare" || action == "cleanup" || action == "check" {
		ctx = w.InitThread(context.Background(), index)
	} else {
		ctx = w.InitThread(timeoutCtx, index)
	}
	defer w.CleanupThread(ctx, index)

	switch action {
	case "prepare":
		// Do cleanup only if dropData is set and not generate csv data.
		if dropData {
			if err := w.Cleanup(ctx, index); err != nil {
				return err
			}
		}
		return w.Prepare(ctx, index)
	case "cleanup":
		return w.Cleanup(ctx, index)
	case "check":
		return w.Check(ctx, index)
	}

	// This loop is only reached for "run" action since other actions return earlier
	for i := 0; i < count || count <= 0; i++ {
		// Check if timeout has occurred before starting next query
		select {
		case <-ctx.Done():
			if !silence {
				fmt.Printf("[%s] %s worker %d stopped due to timeout after %d iterations\n",
					time.Now().Format("2006-01-02 15:04:05"), action, index, i)
			}
			return nil
		default:
		}

		err := w.Run(ctx, index)
		if err != nil {
			// Check if the error is due to timeout/cancellation
			if ctx.Err() != nil {
				if !silence {
					fmt.Printf("[%s] %s worker %d stopped due to timeout: %v\n",
						time.Now().Format("2006-01-02 15:04:05"), action, index, err)
				}
				return nil // Don't treat timeout as an error
			}

			if !silence {
				fmt.Printf("[%s] execute %s failed, err %v\n", time.Now().Format("2006-01-02 15:04:05"), action, err)
			}
			if !ignoreError {
				return err
			}
		}
	}

	return nil
}

func executeWorkload(ctx context.Context, w workload.Workloader, threads int, action string) {
	var wg sync.WaitGroup
	wg.Add(threads)

	outputCtx, outputCancel := context.WithCancel(ctx)
	ch := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(outputInterval)
		defer ticker.Stop()

		for {
			select {
			case <-outputCtx.Done():
				ch <- struct{}{}
				return
			case <-ticker.C:
				w.OutputStats(false)
			}
		}
	}()
	if w.Name() == "tpch" && action == "run" {
		err := w.Exec(`create or replace view revenue0 (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= '1997-07-01'
		and l_shipdate < date_add('1997-07-01', interval '3' month)
	group by
		l_suppkey;`)
		if err != nil {
			panic(fmt.Sprintf("a fatal occurred when preparing view data: %v", err))
		}
	}
	enabledDumpPlanReplayer := w.IsPlanReplayerDumpEnabled()
	if enabledDumpPlanReplayer {
		err := w.PreparePlanReplayerDump()
		if err != nil {
			fmt.Printf("[%s] prepare plan replayer failed, err%v\n",
				time.Now().Format("2006-01-02 15:04:05"), err)
		}
		defer func() {
			err = w.FinishPlanReplayerDump()
			if err != nil {
				fmt.Printf("[%s] dump plan replayer failed, err%v\n",
					time.Now().Format("2006-01-02 15:04:05"), err)
			}
		}()
	}

	for i := 0; i < threads; i++ {
		go func(index int) {
			defer wg.Done()
			if err := execute(ctx, w, action, threads, index); err != nil {
				if action == "prepare" {
					panic(fmt.Sprintf("a fatal occurred when preparing data: %v", err))
				}
				fmt.Printf("execute %s failed, err %v\n", action, err)
				return
			}
		}(i)
	}

	wg.Wait()

	if action == "prepare" {
		// For prepare, we must check the data consistency after all prepare finished
		checkPrepare(ctx, w)
	}
	outputCancel()

	<-ch
}
