package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestCheckPrepareReturnsCheckError(t *testing.T) {
	oldThreads := threads
	oldNoCheck := tpccConfig.NoCheck
	defer func() {
		threads = oldThreads
		tpccConfig.NoCheck = oldNoCheck
	}()

	threads = 2
	tpccConfig.NoCheck = false
	wantErr := errors.New("check failed")
	w := &checkPrepareWorkloader{name: "tpcc", checkErr: wantErr}

	err := checkPrepare(context.Background(), w)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected checkPrepare to return %v, got %v", wantErr, err)
	}
	if got := w.initCalls.Load(); got != int32(threads) {
		t.Fatalf("expected InitThread to be called %d times, got %d", threads, got)
	}
	if got := w.cleanupCalls.Load(); got != int32(threads) {
		t.Fatalf("expected CleanupThread to be called %d times, got %d", threads, got)
	}
}

type checkPrepareWorkloader struct {
	name         string
	checkErr     error
	initCalls    atomic.Int32
	cleanupCalls atomic.Int32
}

func (w *checkPrepareWorkloader) Name() string {
	return w.name
}

func (w *checkPrepareWorkloader) InitThread(ctx context.Context, _ int) context.Context {
	w.initCalls.Add(1)
	return ctx
}

func (w *checkPrepareWorkloader) CleanupThread(_ context.Context, _ int) {
	w.cleanupCalls.Add(1)
}

func (w *checkPrepareWorkloader) Prepare(context.Context, int) error {
	return nil
}

func (w *checkPrepareWorkloader) CheckPrepare(context.Context, int) error {
	return w.checkErr
}

func (w *checkPrepareWorkloader) Run(context.Context, int) error {
	return nil
}

func (w *checkPrepareWorkloader) Cleanup(context.Context, int) error {
	return nil
}

func (w *checkPrepareWorkloader) Check(context.Context, int) error {
	return nil
}

func (w *checkPrepareWorkloader) OutputStats(bool) {
}

func (w *checkPrepareWorkloader) DBName() string {
	return ""
}

func (w *checkPrepareWorkloader) IsPlanReplayerDumpEnabled() bool {
	return false
}

func (w *checkPrepareWorkloader) PreparePlanReplayerDump() error {
	return nil
}

func (w *checkPrepareWorkloader) FinishPlanReplayerDump() error {
	return nil
}

func (w *checkPrepareWorkloader) Exec(string) error {
	return nil
}
