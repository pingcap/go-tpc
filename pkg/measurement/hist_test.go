package measurement

import (
	"math/rand"
	"testing"
	"time"
)

func TestHist(t *testing.T) {
	h := NewHistogram(1*time.Millisecond, 20*time.Minute, 1)
	for i := 0; i < 10000; i++ {
		n := rand.Intn(15020)
		h.Measure(time.Millisecond * time.Duration(n))
	}
	h.Measure(time.Minute * 9)
	h.Measure(time.Minute * 8)
	t.Logf(h.Summary())
}
