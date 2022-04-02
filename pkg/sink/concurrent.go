package sink

import (
	"context"
	"sync"

	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

// ConcurrentSink inserts values to one of the downstream sinks.
// The insert will be blocked if all downstream sinks are working.
//
// WARN: Although this sink can transform serial Writes to multiple down stream sinks, this sink itself is not
// concurrent safe. You must not call WriteRow and Flush concurrently.
type ConcurrentSink struct {
	allSinks []Sink

	writeCh       chan writeRowOp
	writeResultCh chan error
	writeWg       sync.WaitGroup

	concurrentGuard atomic.Int32 // Used to check whether this struct is used concurrently
}

type writeRowOp struct {
	ctx    context.Context
	values []interface{}
}

var _ Sink = &ConcurrentSink{}

func NewConcurrentSink(downStreamBuilder func(idx int) Sink, concurrency int) *ConcurrentSink {
	sinks := make([]Sink, concurrency)
	for i := 0; i < concurrency; i++ {
		sinks[i] = downStreamBuilder(i)
	}

	cs := &ConcurrentSink{
		allSinks:      sinks,
		writeCh:       make(chan writeRowOp, concurrency),
		writeResultCh: make(chan error, 1),
	}
	for i := 0; i < concurrency; i++ {
		go cs.runConsumerLoop(i)
	}
	return cs
}

func (c *ConcurrentSink) runConsumerLoop(downStreamIdx int) {
	sink := c.allSinks[downStreamIdx]

	for {
		select {
		case op, ok := <-c.writeCh:
			if !ok {
				// Channel close
				return
			}
			err := sink.WriteRow(op.ctx, op.values...)
			c.writeWg.Add(-1)
			if err != nil {
				select {
				case c.writeResultCh <- err:
				default:
				}
			}
		}
	}
}

func (c *ConcurrentSink) WriteRow(ctx context.Context, values ...interface{}) error {
	v := c.concurrentGuard.Inc()
	if v > 1 {
		panic("ConcurrentSink cannot be called concurrently")
	}
	defer c.concurrentGuard.Dec()

	c.writeWg.Add(1)
	c.writeCh <- writeRowOp{
		ctx:    ctx,
		values: values,
	}
	select {
	case err := <-c.writeResultCh:
		return err
	default:
		return nil
	}
}

// Flush flushes all downstream sinks concurrently, wait all sinks to be flushed and returns the first error
// encountered.
//
// WARN: Flush() will wait until all existing write ops are finished.
func (c *ConcurrentSink) Flush(ctx context.Context) error {
	v := c.concurrentGuard.Inc()
	if v > 1 {
		panic("ConcurrentSink cannot be called concurrently")
	}
	defer c.concurrentGuard.Dec()

	// Wait all writes to finish.
	c.writeWg.Wait()

	// At this time there is no running write ops, so we are safe to call sink.Flush() for each sink.
	g, ctx := errgroup.WithContext(ctx)
	for _, sink_ := range c.allSinks {
		sink := sink_
		g.Go(func() error {
			return sink.Flush(ctx)
		})
	}
	return g.Wait()
}

// Close closes all downstream sinks concurrently, wait all sinks to be closed and returns the first error
// encountered.
//
// WARN: Close() will wait until all existing write ops are finished.
func (c *ConcurrentSink) Close(ctx context.Context) error {
	v := c.concurrentGuard.Inc()
	if v > 1 {
		panic("ConcurrentSink cannot be called concurrently")
	}
	defer c.concurrentGuard.Dec()

	// Wait all writes to finish.
	c.writeWg.Wait()

	g := new(errgroup.Group)
	for _, sink_ := range c.allSinks {
		sink := sink_
		g.Go(func() error {
			return sink.Close(ctx)
		})
	}
	return g.Wait()
}
