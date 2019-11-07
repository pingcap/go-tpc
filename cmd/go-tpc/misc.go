package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/siddontang/go-tpc/pkg/workload"
)

func execute(ctx context.Context, w workload.Workloader, action string, index int) error {
	count := totalCount / threads

	ctx = w.InitThread(ctx, index)
	defer w.CleanupThread(ctx, index)

	switch action {
	case "prepare":
		if dropData {
			if err := w.Cleanup(ctx, index); err != nil {
				return err
			}
		}
		return w.Prepare(ctx, index)
	case "cleanup":
		return w.Cleanup(ctx, index)
	}

	for i := 0; i < count; i++ {
		if err := w.Run(ctx, index); err != nil {
			if ignoreError {
				fmt.Printf("execute %s failed, err %v\n", action, err)
				continue
			}
			return err
		}
	}

	return nil
}

func executeWorkload(ctx context.Context, w workload.Workloader, action string) {
	var wg sync.WaitGroup

	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go func(index int) {
			defer wg.Done()
			if err := execute(ctx, w, action, index); err != nil {
				fmt.Printf("execute %s failed, err %v\n", action, err)
				return
			}
		}(i)
	}

	wg.Wait()
}
