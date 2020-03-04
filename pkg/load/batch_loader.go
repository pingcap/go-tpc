package load

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"strings"
	"sync"

	"github.com/pingcap/go-tpc/pkg/util"
)

const (
	maxBatchCount = 1024
)

type BatchLoader interface {
	InsertValue(ctx context.Context, query string) error
	Flush(ctx context.Context) error
}

// SQLBatchLoader helps us insert in batch
type SQLBatchLoader struct {
	insertHint string
	conn       *sql.Conn
	buf        bytes.Buffer
	count      int
}

// NewSQLBatchLoader creates a batch loader for sql database
func NewSQLBatchLoader(conn *sql.Conn, hint string) *SQLBatchLoader {
	return &SQLBatchLoader{
		count:      0,
		insertHint: hint,
		conn:       conn,
	}
}

// InsertValue inserts an value, the loader may flush all pending values.
func (b *SQLBatchLoader) InsertValue(ctx context.Context, query string) error {
	sep := ", "
	if b.count == 0 {
		b.buf.WriteString(b.insertHint)
		sep = " "
	}
	b.buf.WriteString(sep)
	b.buf.WriteString("(" + query + ")")

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

// BulkLoader helps us insert in batch
type CSVBatchLoader struct {
	buf        [][]string
	writer     *csv.Writer
	mutex      *sync.Mutex
}

// NewCSVBatchLoader creates a batch loader for csv format
func NewCSVBatchLoader(flock *util.Flock) *CSVBatchLoader {
	return &CSVBatchLoader{
		buf:    make([][]string, 0, maxBatchCount),
		writer: csv.NewWriter(flock.File),
		mutex:  flock.Mutex,
	}
}

// InsertValue inserts an Insert value, the loader may flush all pending values.
func (b *CSVBatchLoader) InsertValue(ctx context.Context, query string) error {
	fields := strings.Split(query, ",  ")
	for i, field := range fields {
		fields[i] = strings.Trim(field, `'`)
	}
	b.buf = append(b.buf, fields)

	if len(b.buf) >= maxBatchCount {
		return b.Flush(ctx)
	}

	return nil
}

// Flush inserts all pending values
func (b *CSVBatchLoader) Flush(ctx context.Context) error {
	if len(b.buf) == 0 {
		return nil
	}

	b.mutex.Lock()
	err := b.writer.WriteAll(b.buf)
	b.buf = b.buf[:0]
	b.writer.Flush()
	b.mutex.Unlock()

	return err
}
