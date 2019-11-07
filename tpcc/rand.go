package tpcc

import (
	"math/rand"
	"time"

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

var (
	cLoad       int
	cCustomerID int
	cItemID     int
)

var cLastTokens = [...]string{
	"BAR", "OUGHT", "ABLE", "PRI", "PRES",
	"ESE", "ANTI", "CALLY", "ATION", "EING"}

func randCLastSyllables(n int, b *util.BufAllocator) string {
	// 3 tokens * max len
	buf := b.Alloc(3 * 5)
	buf = buf[:0]
	buf = append(buf, cLastTokens[n/100]...)
	n = n % 100
	buf = append(buf, cLastTokens[n/10]...)
	n = n % 10
	buf = append(buf, cLastTokens[n]...)
	return util.String(buf)
}

func init() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	cLoad = r.Intn(256)
	cItemID = r.Intn(1024)
	cCustomerID = r.Intn(8192)
}

// refer 4.3.2.3 and 2.1.6
func randCLast(r *rand.Rand, b *util.BufAllocator) string {
	return randCLastSyllables(((r.Intn(256)|r.Intn(1000))+cLoad)%1000, b)
}

// refer 2.1.6
func randCustomerID(r *rand.Rand) int {
	return ((r.Intn(1024) | (r.Intn(3000) + 1) + cCustomerID) % 3000) + 1
}

// refer 2.1.6
func randItemID(r *rand.Rand) int {
	return ((r.Intn(8190) | (r.Intn(100000) + 1) + cItemID) % 100000) + 1
}
