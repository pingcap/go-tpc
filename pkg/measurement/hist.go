package measurement

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Histogram struct {
	m           sync.RWMutex
	bucketCount []int64
	buckets     []int
	count       int64
	sum         int64
	startTime   time.Time
}

type HistInfo struct {
	Elapsed float64
	Sum     int64
	Count   int64
	Ops     float64
	Avg     int64
	P90     int64
	P99     int64
	P999    int64
}

func NewHistogram() *Histogram {
	h := new(Histogram)
	h.startTime = time.Now()
	// Unit 1ms
	h.buckets = []int{1, 2, 4, 8, 9, 12, 16, 20, 24, 32, 40, 48, 64, 80,
		96, 112, 128, 160, 192, 256, 512, 1000, 1500, 2000, 4000, 8000, 16000}
	h.bucketCount = make([]int64, len(h.buckets))
	return h
}

func (h *Histogram) Measure(latency time.Duration) {
	n := int64(latency / time.Millisecond)

	i := sort.SearchInts(h.buckets, int(n))
	if i >= len(h.buckets) {
		i = len(h.buckets) - 1
	}

	h.m.Lock()
	defer h.m.Unlock()

	h.sum += n
	h.count += 1

	h.bucketCount[i] += 1
}

func (h *Histogram) Empty() bool {
	h.m.Lock()
	defer h.m.Unlock()
	return h.count == 0
}

func (h *Histogram) Summary() string {
	res := h.GetInfo()

	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("Takes(s): %.1f, ", res.Elapsed))
	buf.WriteString(fmt.Sprintf("Count: %d, ", res.Count))
	buf.WriteString(fmt.Sprintf("TPM: %.1f, ", res.Ops*60))
	buf.WriteString(fmt.Sprintf("Sum(ms): %d, ", res.Sum))
	buf.WriteString(fmt.Sprintf("Avg(ms): %d, ", res.Avg))
	buf.WriteString(fmt.Sprintf("90th(ms): %d, ", res.P90))
	buf.WriteString(fmt.Sprintf("99th(ms): %d, ", res.P99))
	buf.WriteString(fmt.Sprintf("99.9th(ms): %d", res.P999))

	return buf.String()
}

func (h *Histogram) GetInfo() HistInfo {
	elapsed := time.Now().Sub(h.startTime).Seconds()

	per90 := int64(0)
	per99 := int64(0)
	per999 := int64(0)
	opCount := int64(0)

	h.m.RLock()
	defer h.m.RUnlock()

	sum := h.sum
	count := h.count

	avg := int64(float64(sum) / float64(count))

	for i, hc := range h.bucketCount {
		opCount += hc
		per := float64(opCount) / float64(count)
		if per90 == 0 && per >= 0.90 {
			per90 = int64(h.buckets[i])
		}

		if per99 == 0 && per >= 0.99 {
			per99 = int64(h.buckets[i])
		}

		if per999 == 0 && per >= 0.999 {
			per999 = int64(h.buckets[i])
		}
	}

	ops := float64(count) / elapsed
	info := HistInfo{
		Elapsed: elapsed,
		Sum:     sum,
		Count:   count,
		Ops:     ops,
		Avg:     avg,
		P90:     per90,
		P99:     per99,
		P999:    per999,
	}
	return info
}
