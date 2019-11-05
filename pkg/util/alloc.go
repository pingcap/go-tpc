package util

// BufAllocator helps you manage multi chunks in one []byte.
type BufAllocator struct {
	buf    []byte
	offset int
}

const defaultBufSize = 1024

// NewBufAllocator creates a NewBufAllocator
func NewBufAllocator() *BufAllocator {
	return &BufAllocator{
		buf:    make([]byte, defaultBufSize),
		offset: 0,
	}
}

func (b *BufAllocator) grow(n int) {
	length := len(b.buf) - b.offset
	length = 2 * length

	if length < n {
		length = n
	}

	if length < defaultBufSize {
		length = defaultBufSize
	}

	b.buf = make([]byte, length)
	b.offset = 0
}

// Alloc allocates a new chunk with the specified size n.
func (b *BufAllocator) Alloc(n int) []byte {
	if len(b.buf)-b.offset < n {
		b.grow(n)
	}

	buf := b.buf[b.offset : n+b.offset]
	b.offset += n
	return buf
}

// Reset resets the buffer to later reuse
func (b *BufAllocator) Reset() {
	b.offset = 0
}
