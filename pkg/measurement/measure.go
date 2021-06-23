package measurement

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	sigFigs           = 1
	defaultMinLatency = 1 * time.Millisecond
	DefaultMaxLatency = 16 * time.Second
)

type Measurement struct {
	warmUp int32 // use as bool, 1 means in warmup progress, 0 means warmup finished.
	sync.RWMutex

	MinLatency       time.Duration
	MaxLatency       time.Duration
	SigFigs          int
	OpCurMeasurement map[string]*Histogram
	OpSumMeasurement map[string]*Histogram
}

func (m *Measurement) getHist(op string, err error, current bool) *Histogram {
	opMeasurement := m.OpSumMeasurement
	if current {
		opMeasurement = m.OpCurMeasurement
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
		opM = NewHistogram(m.MinLatency, m.MaxLatency, m.SigFigs)
		opPairedM := NewHistogram(m.MinLatency, m.MaxLatency, m.SigFigs)
		m.Lock()
		opMeasurement[op] = opM
		opMeasurement[opPairedKey] = opPairedM
		m.Unlock()
	}
	return opM
}

func (m *Measurement) measure(op string, err error, lan time.Duration) {
	m.getHist(op, err, true).Measure(lan)
	m.getHist(op, err, false).Measure(lan)
}

func (m *Measurement) takeCurMeasurement() (ret map[string]*Histogram) {
	m.RLock()
	defer m.RUnlock()
	ret, m.OpCurMeasurement = m.OpCurMeasurement, make(map[string]*Histogram, 16)
	return
}

func (m *Measurement) getOpName() []string {
	m.RLock()
	defer m.RUnlock()

	res := make([]string, 0, len(m.OpSumMeasurement))
	for op := range m.OpSumMeasurement {
		res = append(res, op)
	}
	return res
}

// Output prints the measurement summary.
func (m *Measurement) Output(ifSummaryReport bool, outputFunc func(string, map[string]*Histogram)) {
	if ifSummaryReport {
		m.RLock()
		defer m.RUnlock()
		outputFunc("[Summary] ", m.OpSumMeasurement)
		return
	}
	// Clear current measure data every time
	var opCurMeasurement = m.takeCurMeasurement()
	m.RLock()
	defer m.RUnlock()
	currentTime := time.Now()
	var sb strings.Builder
	sb.WriteString("[")
	sb.WriteString(currentTime.Format("2006-01-02 15:04:05"))
	sb.WriteString("]")
	outputFunc(sb.String(), opCurMeasurement)
}

// EnableWarmUp sets whether to enable warm-up.
func (m *Measurement) EnableWarmUp(b bool) {
	if b {
		atomic.StoreInt32(&m.warmUp, 1)
	} else {
		atomic.StoreInt32(&m.warmUp, 0)
	}
}

// IsWarmUpFinished returns whether warm-up is finished or not.
func (m *Measurement) IsWarmUpFinished() bool {
	return atomic.LoadInt32(&m.warmUp) == 0
}

// Measure measures the operation.
func (m *Measurement) Measure(op string, lan time.Duration, err error) {
	if !m.IsWarmUpFinished() {
		return
	}
	m.measure(op, err, lan)
}

func NewMeasurement(opts ...func(*Measurement)) *Measurement {
	m := &Measurement{
		warmUp:           0,
		RWMutex:          sync.RWMutex{},
		MinLatency:       defaultMinLatency,
		MaxLatency:       DefaultMaxLatency,
		SigFigs:          sigFigs,
		OpCurMeasurement: make(map[string]*Histogram, 16),
		OpSumMeasurement: make(map[string]*Histogram, 16),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m
}
