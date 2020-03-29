package measurement

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type measurement struct {
	sync.RWMutex

	outputCounter    int64
	opCurMeasurement map[string]*histogram
	opSumMeasurement map[string]*histogram
}

func (m *measurement) getHist(op string, err error, current bool) *histogram {
	opMeasurement := m.opSumMeasurement
	if current {
		opMeasurement = m.opCurMeasurement
	}

	// Create hist of {op} and {op}_ERR at the same time, or else the TPM would be incorrect
	opPairedKey := fmt.Sprintf("%s_ERR", op)
	if err != nil {
		op, opPairedKey = opPairedKey, op
	}

	m.RLock()
	opM, ok := opMeasurement[op]
	m.RUnlock()
	if !ok {
		opM = newHistogram()
		opPairedM := newHistogram()
		m.Lock()
		opMeasurement[op] = opM
		opMeasurement[opPairedKey] = opPairedM
		m.Unlock()
	}
	return opM
}

func (m *measurement) measure(op string, err error, lan time.Duration) {
	m.getHist(op, err, true).Measure(lan)
	m.getHist(op, err, false).Measure(lan)
}

func (m *measurement) takeCurMeasurement() (ret map[string]*histogram) {
	m.RLock()
	defer m.RUnlock()
	ret, m.opCurMeasurement = m.opCurMeasurement, make(map[string]*histogram, 16)
	return
}

func outputMeasurement(opMeasurement map[string]*histogram, prefix string) {
	keys := make([]string, len(opMeasurement))
	var i = 0
	for k := range opMeasurement {
		keys[i] = k
		i += 1
	}
	sort.Strings(keys)

	for _, op := range keys {
		hist := opMeasurement[op]
		if !hist.Empty() {
			fmt.Printf("%s%-6s - %s\n", prefix, strings.ToUpper(op), hist.Summary())
		}
	}
}

func (m *measurement) output(summaryReport bool) {
	// Clear current measure data every time
	var opCurMeasurement = m.takeCurMeasurement()

	if summaryReport {
		m.RLock()
		defer m.RUnlock()
		outputMeasurement(m.opSumMeasurement, "[SUM] ")
	} else {
		outputMeasurement(opCurMeasurement, "[CUR] ")
		m.RLock()
		defer m.RUnlock()
		m.outputCounter += 1
		if m.outputCounter%10 == 0 {
			outputMeasurement(m.opSumMeasurement, "[SUM] ")
		}
	}
}

func (m *measurement) getOpName() []string {
	m.RLock()
	defer m.RUnlock()

	res := make([]string, 0, len(m.opSumMeasurement))
	for op := range m.opSumMeasurement {
		res = append(res, op)
	}
	return res
}

// Output prints the measurement summary.
func Output(summaryReport bool) {
	globalMeasure.output(summaryReport)
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
	globalMeasure.measure(op, err, lan)
}

func newMeasurement() *measurement {
	return &measurement{
		sync.RWMutex{},
		0,
		make(map[string]*histogram, 16),
		make(map[string]*histogram, 16),
	}
}

var (
	globalMeasure *measurement
	warmUp        int32 // use as bool, 1 means in warmup progress, 0 means warmup finished.
)

func init() {
	globalMeasure = newMeasurement()
	warmUp = 0
}
