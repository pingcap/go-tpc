package measurement

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type measurement struct {
	sync.RWMutex

	opMeasurement map[string]*histogram
}

func (m *measurement) measure(op string, lan time.Duration) {
	m.RLock()
	opM, ok := m.opMeasurement[op]
	m.RUnlock()

	if !ok {
		opM = newHistogram()
		m.Lock()
		m.opMeasurement[op] = opM
		m.Unlock()
	}

	opM.Measure(lan)
}

func (m *measurement) output() {
	m.RLock()
	defer m.RUnlock()
	keys := make([]string, len(m.opMeasurement))
	var i = 0
	for k := range m.opMeasurement {
		keys[i] = k
		i += 1
	}
	sort.Strings(keys)

	for _, op := range keys {
		fmt.Printf("%-6s - %s\n", op, m.opMeasurement[op].Summary())
	}
}

func (m *measurement) getOpName() []string {
	m.RLock()
	defer m.RUnlock()

	res := make([]string, 0, len(m.opMeasurement))
	for op := range m.opMeasurement {
		res = append(res, op)
	}
	return res
}

// Output prints the measurement summary.
func Output() {
	globalMeasure.output()
}

// EnableWarmUp sets whether to enable warm-up.
func EnableWarmUp(b bool) {
	if b {
		atomic.StoreInt32(&warmUp, 1)
	} else {
		atomic.StoreInt32(&warmUp, 0)
	}
}

// IsWarmUpFinished returns whether warm-up is finished or not.
func IsWarmUpFinished() bool {
	return atomic.LoadInt32(&warmUp) == 0
}

// Measure measures the operation.
func Measure(op string, lan time.Duration, err error) {
	if !IsWarmUpFinished() {
		return
	}

	if err != nil {
		op = fmt.Sprintf("%s-ERR", op)
	}

	globalMeasure.measure(op, lan)
}

var (
	globalMeasure *measurement
	warmUp        int32 // use as bool, 1 means in warmup progress, 0 means warmup finished.
)

func init() {
	warmUp = 0
	globalMeasure = new(measurement)
	globalMeasure.opMeasurement = make(map[string]*histogram, 16)
}
