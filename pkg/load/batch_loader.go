package load

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"os"
)

const (
	maxBatchCount = 1024
)

type BatchLoader interface {
	InsertValue(ctx context.Context, query []string) error
	Flush(ctx context.Context) error
}

// SQLBatchLoader helps us insert in batch
type SQLBatchLoader struct {
	insertHint string
	conn       *sql.Conn
	buf        bytes.Buffer
	count      int
}

// NewSQLBatchLoader creates a batch loader for database connection
func NewSQLBatchLoader(conn *sql.Conn, hint string) *SQLBatchLoader {
	return &SQLBatchLoader{
		count:      0,
		insertHint: hint,
		conn:       conn,
	}
}

// InsertValue inserts a value, the loader may flush all pending values.
func (b *SQLBatchLoader) InsertValue(ctx context.Context, query []string) error {
	sep := ", "
	if b.count == 0 {
		b.buf.WriteString(b.insertHint)
		sep = " "
	}
	b.buf.WriteString(sep)
	b.buf.WriteString(query[0])

	b.count++

	if b.count >= maxBatchCount {
		return b.Flush(ctx)
	}

	return nil
}

// Flush inserts all pending values
func (b *SQLBatchLoader) Flush(ctx context.Context) error {
	if b.buf.Len() == 0 {
		return nil
	}

	_, err := b.conn.ExecContext(ctx, b.buf.String())
	b.count = 0
	b.buf.Reset()

	return err
}

// CSVBatchLoader helps us insert in batch
type CSVBatchLoader struct {
	f      *os.File
	writer *csv.Writer
}

// NewCSVBatchLoader creates a batch loader for csv format
func NewCSVBatchLoader(f *os.File) *CSVBatchLoader {
	return &CSVBatchLoader{
		f:      f,
		writer: csv.NewWriter(f),
	}
}

// InsertValue inserts a value, the loader may flush all pending values.
func (b *CSVBatchLoader) InsertValue(ctx context.Context, columns []string) error {
	return b.writer.Write(columns)
}

// Flush inserts all pending values
func (b *CSVBatchLoader) Flush(ctx context.Context) error {
	b.writer.Flush()
	return nil
}

// Close closes the file.
func (b *CSVBatchLoader) Close(ctx context.Context) error {
	return b.f.Close()
}
