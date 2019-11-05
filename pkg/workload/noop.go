package workload

import (
	"context"
)

// NoopWorkloader is a Noop workloader and does nothing
type NoopWorkloader struct {
}

// Name implements Workloader interface
func (w NoopWorkloader) Name() string {
	return "noop"
}

// InitThread implements Workloader interface
func (w NoopWorkloader) InitThread(ctx context.Context, threadID int) context.Context {
	return ctx
}

// CleanupThread implements Workloader interface
func (w NoopWorkloader) CleanupThread(ctx context.Context, threadID int) {}

// Prepare implements Workloader interface
func (w NoopWorkloader) Prepare(ctx context.Context, threadID int) error {
	return nil
}

// Run implements Workloader interface
func (w NoopWorkloader) Run(ctx context.Context, threadID int) error {
	return nil
}

// Cleanup implements Workloader interface
func (w NoopWorkloader) Cleanup(ctx context.Context, threadID int) error {
	return nil
}
