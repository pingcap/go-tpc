package sink

import (
	"context"
)

type Sink interface {
	WriteRow(ctx context.Context, values ...interface{}) error
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
}
