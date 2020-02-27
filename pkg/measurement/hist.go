package measurement

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"
)

type histogram struct {
	m           sync.RWMutex
	bucketCount []int64
	buckets     []int
	count       int64
	sum         int64
	startTime   time.Time
}

type histInfo struct {
	elapsed float64
	sum     int64
	count   int64
	ops     float64
	avg     int64
	p95     int64
	p99     int64
	p999    int64
}

func newHistogram() *histogram {
	h := new(histogram)
	h.startTime = time.Now()
	// Unit 1ms
	h.buckets = []int{1, 2, 4, 8, 9, 12, 16, 20, 24, 32, 40, 48, 64, 80,
		96, 112, 128, 160, 192, 256, 512, 1000, 1500, 2000, 4000, 8000, 16000}
	h.bucketCount = make([]int64, len(h.buckets))
	return h
}

func (h *histogram) Measure(latency time.Duration) {
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

func (h *histogram) Summary() string {
	res := h.getInfo()

	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("Takes(s): %.1f, ", res.elapsed))
	buf.WriteString(fmt.Sprintf("Count: %d, ", res.count))
	buf.WriteString(fmt.Sprintf("OPS: %.1f, ", res.ops))
	buf.WriteString(fmt.Sprintf("Sum(ms): %d, ", res.sum))
	buf.WriteString(fmt.Sprintf("Avg(ms): %d, ", res.avg))
	buf.WriteString(fmt.Sprintf("95th(ms): %d, ", res.p95))
	buf.WriteString(fmt.Sprintf("99th(ms): %d, ", res.p99))
	buf.WriteString(fmt.Sprintf("99.9th(ms): %d", res.p999))

	return buf.String()
}

func (h *histogram) getInfo() histInfo {
	elapsed := time.Now().Sub(h.startTime).Seconds()

	per95 := int64(0)
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
		if per95 == 0 && per >= 0.95 {
			per95 = int64(h.buckets[i])
		}

		if per99 == 0 && per >= 0.99 {
			per99 = int64(h.buckets[i])
		}

		if per999 == 0 && per >= 0.999 {
			per999 = int64(h.buckets[i])
		}
	}

	ops := float64(count) / elapsed
	info := histInfo{
		elapsed: elapsed,
		sum:     sum,
		count:   count,
		ops:     ops,
		avg:     avg,
		p95:     per95,
		p99:     per99,
		p999:    per999,
	}
	return info
}
