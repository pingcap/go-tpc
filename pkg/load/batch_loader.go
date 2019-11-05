package load

import (
	"bytes"
	"context"
	"database/sql"
)

const (
	maxBatchCount = 1024
)

// BulkLoader helps us insert in batch
type BatchLoader struct {
	insertHint string
	conn       *sql.Conn
	buf        bytes.Buffer
	count      int
}

// NewBatchLoader creates a batch loader
func NewBatchLoader(conn *sql.Conn, hint string) *BatchLoader {
	return &BatchLoader{
		count:      0,
		insertHint: hint,
		conn:       conn,
	}
}

// Insert inserts an Insert value, the loader may flush all pending values.
func (b *BatchLoader) InsertValue(ctx context.Context, query string) error {
	sep := ", "
	if b.count == 0 {
		b.buf.WriteString(b.insertHint)
		sep = " "
	}
	b.buf.WriteString(sep)
	b.buf.WriteString(query)

	b.count++

	if b.count >= maxBatchCount {
		return b.Flush(ctx)
	}

	return nil
}

// Flush inserts all pending values
func (b *BatchLoader) Flush(ctx context.Context) error {
	if b.buf.Len() == 0 {
		return nil
	}

	_, err := b.conn.ExecContext(ctx, b.buf.String())
	b.count = 0
	b.buf.Reset()
	return err
}
