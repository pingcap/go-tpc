package sink

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
)

var dummySinkCtxKey = struct{}{}

// dummySink simply forwards all received payload to the output channel.
type dummySink struct {
	concurrentGuard  atomic.Int32 // Used to check whether there are concurrent violations
	receivedCalls    chan callDesc
	maxWorkloadMs    int
	totalProcessedMs *atomic.Int64
}

type callDesc struct {
	write int
	flush int
}

var _ Sink = &dummySink{}

func (s *dummySink) doWorkload() {
	if s.maxWorkloadMs > 0 {
		// Simulate a processing delay.
		delayMs := rand.Int63n(int64(s.maxWorkloadMs))
		s.totalProcessedMs.Add(delayMs)
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
	}
}

func (s *dummySink) WriteRow(ctx context.Context, values ...interface{}) error {
	v := s.concurrentGuard.Inc()
	if v > 1 {
		panic("dummySink must not called concurrently")
	}
	defer s.concurrentGuard.Dec()

	s.receivedCalls <- callDesc{write: ctx.Value(dummySinkCtxKey).(int)}
	s.doWorkload()
	return nil
}

func (s *dummySink) Flush(ctx context.Context) error {
	v := s.concurrentGuard.Inc()
	if v > 1 {
		panic("dummySink must not called concurrently")
	}
	defer s.concurrentGuard.Dec()

	s.receivedCalls <- callDesc{flush: ctx.Value(dummySinkCtxKey).(int)}
	s.doWorkload()
	return nil
}

func (s *dummySink) Close(ctx context.Context) error {
	return s.Flush(ctx)
}

type ConcurrentSinkSuite struct {
	suite.Suite

	numCalls       int
	numConcurrency int
	ratioFlush     float32
	workloadMs     int

	outerCalls                   []callDesc // Calls to the outer (concurrent sink)
	innerCalls                   []callDesc // Calls received from the inner (dummy sink)
	totalProcessedMsPerInnerSink []int64
	totalProcessedMsOuterSink    int64
}

// prepareSimulatingCalls prepare payloads to be sent to the concurrent sink.
func (suite *ConcurrentSinkSuite) prepareSimulatingCalls() []callDesc {
	rand.Seed(time.Now().UnixNano())
	calls := make([]callDesc, suite.numCalls)
	for i := 0; i < suite.numCalls; i++ {
		if rand.Intn(10)+1 <= int(float32(10)*suite.ratioFlush) {
			calls[i] = callDesc{flush: rand.Int()}
		} else {
			calls[i] = callDesc{write: rand.Int()}
		}
	}
	calls = append(calls, callDesc{flush: -1}) // -1 means close
	return calls
}

func (suite *ConcurrentSinkSuite) sendCallsToSink(sink Sink, calls []callDesc) {
	for _, call := range calls {
		if call.write != 0 {
			_ = sink.WriteRow(context.WithValue(context.Background(), dummySinkCtxKey, call.write))
		} else {
			if call.flush == -1 {
				_ = sink.Close(context.WithValue(context.Background(), dummySinkCtxKey, call.flush))
			} else {
				_ = sink.Flush(context.WithValue(context.Background(), dummySinkCtxKey, call.flush))
			}
		}
	}
}

// estimateWorkloadMs returns the upper time (in ms) that the simulated workload will take.
func (suite *ConcurrentSinkSuite) estimateWorkloadMs(calls []callDesc) int {
	nFlushes := 0
	for _, call := range calls {
		if call.flush != 0 {
			nFlushes++
		}
	}
	return nFlushes*2*suite.workloadMs + (suite.numCalls-nFlushes)/suite.numConcurrency*suite.workloadMs
}

// Run workload only once per suite. Multiple checks will be performed.
func (suite *ConcurrentSinkSuite) SetupSuite() {
	outerCalls := suite.prepareSimulatingCalls()
	timeUpperBound := suite.estimateWorkloadMs(outerCalls)
	fmt.Println("Estimated max test time (ms):", timeUpperBound)

	innerCallsCh := make(chan callDesc, suite.numCalls*suite.numConcurrency+1000000)
	processedMsCounters := make([]*atomic.Int64, suite.numConcurrency)

	cs := NewConcurrentSink(func(idx int) Sink {
		processedMsCounters[idx] = atomic.NewInt64(0)
		return &dummySink{
			receivedCalls:    innerCallsCh,
			maxWorkloadMs:    suite.workloadMs,
			totalProcessedMs: processedMsCounters[idx],
		}
	}, suite.numConcurrency)

	timeBegin := time.Now()
	suite.sendCallsToSink(cs, outerCalls)
	suite.totalProcessedMsOuterSink = time.Since(timeBegin).Milliseconds()
	close(innerCallsCh)

	var innerCalls []callDesc
	for val := range innerCallsCh {
		innerCalls = append(innerCalls, val)
	}

	var processedMs []int64
	for _, val := range processedMsCounters {
		processedMs = append(processedMs, val.Load())
	}

	suite.outerCalls = outerCalls
	suite.innerCalls = innerCalls
	suite.totalProcessedMsPerInnerSink = processedMs
}

// TestAllCallsPresent tests whether the downstream sink receives exactly the same set of data that is sent
// to the ConcurrentSink.
func (suite *ConcurrentSinkSuite) TestAllCallsPresent() {
	r := suite.Require()

	// For each flush to ConcurrentSink, there should be N flushes from all downstream sinks.
	// For each write to ConcurrentSink, there should be 1 write from all downstream sinks.
	m := map[int]int{}
	for _, val := range suite.innerCalls {
		if val.write != 0 {
			m[val.write]++
		} else {
			m[val.flush]++
		}
	}
	for _, v := range suite.outerCalls {
		if v.write != 0 {
			r.True(m[v.write] > 0)
			m[v.write]--
			if m[v.write] == 0 {
				delete(m, v.write)
			}
		} else {
			r.True(m[v.flush] > 0)
			m[v.flush] -= suite.numConcurrency
			if m[v.flush] == 0 {
				delete(m, v.flush)
			}
		}
	}
	r.Empty(m)
}

// TestFlushAfterWrite tests whether all writes before the flush are processed before the flush
// from all downstream sinks.
func (suite *ConcurrentSinkSuite) TestFlushAfterWrite() {
	r := suite.Require()

	lastWriteIdx := 0
	remainingInnerCalls := suite.innerCalls

	for flushIdx, outerCall := range suite.outerCalls {
		// Find a flush call
		if outerCall.flush == 0 {
			continue
		}

		// [lastWriteIdx, flushIdx) are all write calls, add them to the map
		m := map[int]int{}
		for writeIdx := lastWriteIdx; writeIdx < flushIdx; writeIdx++ {
			r.True(suite.outerCalls[writeIdx].write != 0)
			m[suite.outerCalls[writeIdx].write]++
		}

		// Check whether we received these write calls in the innerCalls
		expectedFlushes := suite.numConcurrency
		for innerIdx, innerCall := range remainingInnerCalls {
			if innerCall.flush != 0 {
				r.Equal(outerCall.flush, innerCall.flush)
				expectedFlushes--
				if expectedFlushes == 0 {
					// We have met all flushes we want, check whether map is drained
					r.Empty(m)
					remainingInnerCalls = remainingInnerCalls[innerIdx+1:]
					break // Let's check next flush
				}
			} else {
				r.True(innerCall.write != 0)
				r.True(m[innerCall.write] > 0)
				m[innerCall.write]--
				if m[innerCall.write] == 0 {
					delete(m, innerCall.write)
				}
			}
		}

		lastWriteIdx = flushIdx + 1
	}

	r.Empty(remainingInnerCalls)
}

// TestDistribution tests whether the downstream sinks receive tasks evenly.
func (suite *ConcurrentSinkSuite) TestDistribution() {
	// If sample size are small, the distribution will not be even.
	if (suite.numCalls / suite.numConcurrency) < 10 {
		suite.T().SkipNow()
		return
	}

	var sum int64
	for _, val := range suite.totalProcessedMsPerInnerSink {
		sum += val
	}

	avg := sum / int64(suite.numConcurrency)
	for _, val := range suite.totalProcessedMsPerInnerSink {
		delta := math.Abs(float64(avg - val))
		allowedMaxDelta := float64(avg) * 0.1
		suite.Require().LessOrEqual(delta, allowedMaxDelta)
	}
}

func TestConcurrentSink(t *testing.T) {
	suite.Run(t, &ConcurrentSinkSuite{
		numCalls:       20000,
		numConcurrency: 500,
		ratioFlush:     0.4,
		workloadMs:     0,
	})
	suite.Run(t, &ConcurrentSinkSuite{
		numCalls:       2000,
		numConcurrency: 500,
		ratioFlush:     0,
		workloadMs:     0,
	})
	suite.Run(t, &ConcurrentSinkSuite{
		numCalls:       20000,
		numConcurrency: 500,
		ratioFlush:     0,
		workloadMs:     50,
	})
	suite.Run(t, &ConcurrentSinkSuite{
		numCalls:       500,
		numConcurrency: 5,
		ratioFlush:     0.2,
		workloadMs:     50,
	})
	suite.Run(t, &ConcurrentSinkSuite{
		numCalls:       20,
		numConcurrency: 10,
		ratioFlush:     0,
		workloadMs:     50,
	})
	suite.Run(t, &ConcurrentSinkSuite{
		numCalls:       100,
		numConcurrency: 500,
		ratioFlush:     0.1,
		workloadMs:     50,
	})
	suite.Run(t, &ConcurrentSinkSuite{
		numCalls:       30,
		numConcurrency: 1,
		ratioFlush:     0.1,
		workloadMs:     50,
	})
	suite.Run(t, &ConcurrentSinkSuite{
		numCalls:       500,
		numConcurrency: 100,
		ratioFlush:     0.1,
		workloadMs:     50,
	})
}
