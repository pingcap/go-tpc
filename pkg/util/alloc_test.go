package util

import "testing"

func TestBufAlloc(t *testing.T) {
	b := NewBufAllocator()
	orgBuf := b.buf

	buf1 := b.Alloc(100)
	buf1[99] = 'a'

	if orgBuf[99] != 'a' {
		t.Fatalf("expect a, but got %c", orgBuf[99])
	}

	b.Reset()

	buf1 = b.Alloc(100)
	if buf1[99] != 'a' {
		t.Fatalf("expect a, but got %c", buf1[99])
	}

	buf2 := b.Alloc(100)
	buf2[99] = 'b'

	orgBuf[299] = 'd'
	buf3 := b.Alloc(1025)
	buf3[99] = 'c'

	if orgBuf[99] != 'a' {
		t.Fatalf("expect a, but got %c", orgBuf[99])
	}

	if orgBuf[199] != 'b' {
		t.Fatalf("expect b, but got %c", orgBuf[199])
	}

	if orgBuf[299] != 'd' {
		t.Fatalf("expect d, but got %d", orgBuf[299])
	}
}
