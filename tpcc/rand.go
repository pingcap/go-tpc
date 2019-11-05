package tpcc

import (
	"math/rand"

	"github.com/siddontang/go-tpc/pkg/util"
)

// randInt return a random int in [min, max]
// refer 4.3.2.5
func randInt(r *rand.Rand, min, max int) int {
	if min == max {
		return min
	}
	return r.Intn(max-min+1) + min
}

const (
	characters = `abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890`
	letters    = `ABCDEFGHIJKLMNOPQRSTUVWXYZ`
	numbers    = `1234567890`
)

func randBuffer(r *rand.Rand, b *util.BufAllocator, min, max int, s string) []byte {
	buf := b.Alloc(randInt(r, min, max))
	for i := range buf {
		buf[i] = characters[r.Intn(len(s))]
	}
	return buf
}

// refer 4.3.2.2
func randChars(r *rand.Rand, b *util.BufAllocator, min, max int) string {
	return util.String(randBuffer(r, b, min, max, characters))
}

// refer 4.3.2.2
func randLetters(r *rand.Rand, b *util.BufAllocator, min, max int) string {
	return util.String(randBuffer(r, b, min, max, letters))
}

// refer 4.3.2.2
func randNumbers(r *rand.Rand, b *util.BufAllocator, min, max int) string {
	return util.String(randBuffer(r, b, min, max, numbers))
}

// refer 4.3.2.7
func randZip(r *rand.Rand, b *util.BufAllocator) string {
	buf := randBuffer(r, b, 9, 9, numbers)
	copy(buf[4:], `11111`)
	return util.String(buf)
}

func randState(r *rand.Rand, b *util.BufAllocator) string {
	buf := randBuffer(r, b, 2, 2, letters)
	return util.String(buf)
}

func randTax(r *rand.Rand) float64 {
	return float64(randInt(r, 0, 2000)) / float64(10000.0)
}

const originalString = "ORIGINAL"

// refer 4.3.3.1
// random a-string [26 .. 50]. For 10% of the rows, selected at random,
// the string "ORIGINAL" must be held by 8 consecutive characters starting at a random position within buf
func randOriginalString(r *rand.Rand, b *util.BufAllocator) string {
	if r.Intn(10) == 0 {
		buf := randBuffer(r, b, 26, 50, characters)
		index := r.Intn(len(buf) - 8)
		copy(buf[index:], originalString)
		return util.String(buf)
	}

	return randChars(r, b, 26, 50)
}
