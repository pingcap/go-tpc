package workload

import (
	"context"
)

// Workloader is the interface for running customized workload
type Workloader interface {
	Name() string
	InitThread(ctx context.Context, threadID int) context.Context
	CleanupThread(ctx context.Context, threadID int)
	Prepare(ctx context.Context, threadID int) error
	CheckPrepare(ctx context.Context, threadID int) error
	Run(ctx context.Context, threadID int) error
	Cleanup(ctx context.Context, threadID int) error
	Check(ctx context.Context, threadID int) error
	DataGen() bool
	DBName() string
}
