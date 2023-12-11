package dtable

import (
	sqldrv "database/sql/driver"
	"fmt"
	"io"
	"time"
)

type column struct {
	ID        string `json:"id"`
	Key       string `json:"key"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Data      any    `json:"data"`
	TableID   string `json:"table_id"`
	TableName string `json:"table_name"`
}

type dtableRows struct {
	iter       int
	ColumnList []column `json:"metadata"`
	Data       []any    `json:"results"`
}

func (d *dtableRows) Columns() []string {
	keys := make([]string, len(d.ColumnList))
	for i := range d.ColumnList {
		keys[i] = d.ColumnList[i].ID
	}
	return keys
}

func (d *dtableRows) Close() error {
	return nil
}

func (d *dtableRows) Next(dest []sqldrv.Value) error {
	defer func() { d.iter++ }()
	if d.iter >= len(d.Data) {
		return io.EOF
	}
	r := d.Data[d.iter]
	var row map[string]any
	var ok bool
	if row, ok = r.(map[string]any); !ok {
		return fmt.Errorf("invalid row format")
	}
	for i := range dest {
		col := d.ColumnList[i]
		data := row[col.ID]
		val, err := parseData(col, data)
		if err != nil {
			return err
		}
		dest[i] = val
	}
	return nil
}

func parseData(column column, data any) (any, error) {
	if data == nil {
		return nil, nil
	}
	switch column.Type {
	case "text", "long-text":
		return toString(data)
	case "checkbox":
		return toBool(data)
	case "number":
		return toFloat(data)
	case "date", "ctime", "mtime":
		return toTime(data)
	default:
		return toString(data)
	}
}

func toString(data any) (string, error) {
	switch x := data.(type) {
	case string:
		return x, nil
	default:
		return "", fmt.Errorf("the value is not string")
	}
}

func toFloat(data any) (float64, error) {
	switch x := data.(type) {
	case float64:
		return x, nil
	default:
		return 0, fmt.Errorf("the value cannot convert to float")
	}
}

func toBool(data any) (bool, error) {
	switch x := data.(type) {
	case bool:
		return x, nil
	default:
		return false, fmt.Errorf("the value cannot convert to bool")
	}
}

func toTime(data any) (time.Time, error) {
	switch x := data.(type) {
	case time.Time:
		return x, nil
	case string:
		t, err := time.Parse(time.RFC3339Nano, x)
		return t, err
	default:
		return time.Time{}, fmt.Errorf("the value cannot convert to time.Time")
	}
}
