package tpcc

import (
	"context"
)

// Check implements Workloader interface
func (w *Workloader) Check(ctx context.Context, threadID int) error {
	// refer 3.3.2
	return nil
}
