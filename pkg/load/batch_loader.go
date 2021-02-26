package load

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"
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
	db         *sql.DB
	buf        bytes.Buffer
	count      int

	// loader retry
	retryCount    int
	retryDuration time.Duration
}

// NewSQLBatchLoader creates a batch loader for database connection
func NewSQLBatchLoader(db *sql.DB, hint string, retryCount int, retryDuration time.Duration) *SQLBatchLoader {
	return &SQLBatchLoader{
		count:         0,
		insertHint:    hint,
		db:            db,
		retryCount:    retryCount,
		retryDuration: retryDuration,
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

	var err error
	for i := 0; i < 1+b.retryCount; i++ {
		_, err = b.db.ExecContext(ctx, b.buf.String())
		if err == nil || (strings.Contains(err.Error(), "Error 1062: Duplicate entry") && i == 0) {
			break
		}
		if i < b.retryCount {
			fmt.Printf("exec statement error: %v, may try again later...\n", err)
			time.Sleep(b.retryDuration)
		}
	}
	if err != nil {
		return fmt.Errorf("exec statement error: %v", err)
	}

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
	return b.writer.Error()
}

// Close closes the file.
func (b *CSVBatchLoader) Close(ctx context.Context) error {
	return b.f.Close()
}
