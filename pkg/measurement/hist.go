package measurement

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

type Histogram struct {
	*hdrhistogram.Histogram
	m         sync.RWMutex
	sum       int64
	startTime time.Time
}

type HistInfo struct {
	Elapsed float64
	Sum     float64
	Count   int64
	Ops     float64
	Avg     float64
	P50     float64
	P90     float64
	P95     float64
	P99     float64
	P999    float64
	Max     float64
}

func NewHistogram(minLat, maxLat time.Duration, sf int) *Histogram {
	return &Histogram{Histogram: hdrhistogram.New(minLat.Nanoseconds(), maxLat.Nanoseconds(), sf), startTime: time.Now()}
}

func (h *Histogram) Measure(rawLatency time.Duration) {
	latency := rawLatency
	if latency < time.Duration(h.LowestTrackableValue()) {
		latency = time.Duration(h.LowestTrackableValue())
	} else if latency > time.Duration(h.HighestTrackableValue()) {
		latency = time.Duration(h.HighestTrackableValue())
	}
	h.m.Lock()
	err := h.RecordValue(latency.Nanoseconds())
	h.sum += rawLatency.Nanoseconds()
	h.m.Unlock()
	if err != nil {
		panic(fmt.Sprintf(`recording value error: %s`, err))
	}
}

func (h *Histogram) Empty() bool {
	h.m.Lock()
	defer h.m.Unlock()
	return h.TotalCount() == 0
}

func (h *Histogram) Summary() string {
	res := h.GetInfo()
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("Takes(s): %.1f, ", res.Elapsed))
	buf.WriteString(fmt.Sprintf("Count: %d, ", res.Count))
	buf.WriteString(fmt.Sprintf("TPM: %.1f, ", res.Ops*60))
	buf.WriteString(fmt.Sprintf("Sum(ms): %.1f, ", res.Sum))
	buf.WriteString(fmt.Sprintf("Avg(ms): %.1f, ", res.Avg))
	buf.WriteString(fmt.Sprintf("50th(ms): %.1f, ", res.P50))
	buf.WriteString(fmt.Sprintf("90th(ms): %.1f, ", res.P90))
	buf.WriteString(fmt.Sprintf("95th(ms): %.1f, ", res.P95))
	buf.WriteString(fmt.Sprintf("99th(ms): %.1f, ", res.P99))
	buf.WriteString(fmt.Sprintf("99.9th(ms): %.1f, ", res.P999))
	buf.WriteString(fmt.Sprintf("Max(ms): %.1f", res.Max))

	return buf.String()
}

func (h *Histogram) GetInfo() HistInfo {
	h.m.RLock()
	defer h.m.RUnlock()
	sum := time.Duration(h.sum).Seconds() * 1000
	avg := time.Duration(h.Mean()).Seconds() * 1000
	elapsed := time.Now().Sub(h.startTime).Seconds()
	count := h.TotalCount()
	ops := float64(count) / elapsed
	info := HistInfo{
		Elapsed: elapsed,
		Sum:     sum,
		Count:   count,
		Ops:     ops,
		Avg:     avg,
		P50:     time.Duration(h.ValueAtQuantile(50)).Seconds() * 1000,
		P90:     time.Duration(h.ValueAtQuantile(90)).Seconds() * 1000,
		P95:     time.Duration(h.ValueAtQuantile(95)).Seconds() * 1000,
		P99:     time.Duration(h.ValueAtQuantile(99)).Seconds() * 1000,
		P999:    time.Duration(h.ValueAtQuantile(99.9)).Seconds() * 1000,
		Max:     time.Duration(h.ValueAtQuantile(100)).Seconds() * 1000,
	}
	return info
}
