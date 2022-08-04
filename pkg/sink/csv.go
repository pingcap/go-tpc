package sink

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
)

// CSVSink writes values to a file in CSV format.
type CSVSink struct {
	writer     *csv.Writer
	underlying io.Writer
}

var _ Sink = &CSVSink{}

// NewCSVSink creates a sink that writes values to an io.Writer in CSV format.
func NewCSVSink(w io.Writer) *CSVSink {
	return &CSVSink{
		writer:     csv.NewWriter(w),
		underlying: w,
	}
}

// NewCSVSinkWithDelimiter creates a sink that writes values to an io.Writer in CSV format, using a customized delimiter.
func NewCSVSinkWithDelimiter(w io.Writer, delimiter rune) *CSVSink {
	sink := NewCSVSink(w)
	sink.writer.Comma = delimiter
	return sink
}

func buildColumns(values []interface{}) []string {
	columns := make([]string, len(values))
	for i, v := range values {
		ty := reflect.TypeOf(v)
		if ty == nil {
			columns[i] = "NULL"
			continue
		}
		switch ty.Kind() {
		case reflect.String:
			columns[i] = fmt.Sprintf("%s", v)
			continue
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			columns[i] = fmt.Sprintf("%d", v)
			continue
		case reflect.Float32, reflect.Float64:
			columns[i] = fmt.Sprintf("%f", v)
			continue
		}
		switch v := v.(type) {
		case sql.NullString:
			if v.Valid {
				columns[i] = v.String
			} else {
				columns[i] = "NULL"
			}
		case sql.NullInt64:
			if v.Valid {
				columns[i] = fmt.Sprintf("%d", v.Int64)
			} else {
				columns[i] = "NULL"
			}
		case sql.NullFloat64:
			if v.Valid {
				columns[i] = fmt.Sprintf("%f", v.Float64)
			} else {
				columns[i] = "NULL"
			}
		default:
			panic(fmt.Sprintf("unsupported type: %T", v))
		}
	}
	return columns
}

// WriteRow writes a row to the underlying io.Writer. The writing attempt may be deferred until reaching a batch.
func (s *CSVSink) WriteRow(ctx context.Context, values ...interface{}) error {
	columns := buildColumns(values)
	return s.writer.Write(columns)
}

// Flush writes any buffered data to the underlying io.Writer.
func (s *CSVSink) Flush(ctx context.Context) error {
	s.writer.Flush()
	return s.writer.Error()
}

// Close closes the underlying io.Writer if it is an io.WriteCloser.
func (s *CSVSink) Close(ctx context.Context) error {
	if err := s.Flush(ctx); err != nil {
		return err
	}
	if wc, ok := s.underlying.(io.WriteCloser); ok {
		return wc.Close()
	}
	return nil
}
