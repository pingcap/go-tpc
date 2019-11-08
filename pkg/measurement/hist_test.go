package measurement

import (
	"math/rand"
	"testing"
	"time"
)

func TestHist(t *testing.T) {
	h := newHistogram()
	for i := 0; i < 100; i++ {
		n := rand.Intn(100)
		h.Measure(time.Millisecond * time.Duration(n))
	}

	t.Logf(h.Summary())
	// t.Fail()
}
