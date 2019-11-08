package tpcc

import (
	"context"
	"time"
)

func (w *Workloader) runPayment(ctx context.Context, thread int) error {
	s := w.getState(ctx)

	// Sleep just for test, will remove soon before introducing real logic
	time.Sleep(time.Duration(s.R.Intn(100)) * time.Millisecond)
	return nil
}
