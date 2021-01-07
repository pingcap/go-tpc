package util

import (
	"database/sql"
	"github.com/jedib0t/go-pretty/table"
)

func RenderExplainAnalyze(rows *sql.Rows) (text string, err error) {
	table := table.NewWriter()

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	columns := make([]interface{}, len(cols))
	for idx, column := range cols {
		columns[idx] = column
	}
	table.AppendHeader(columns)

	for rows.Next() {
		rawResult := make([][]byte, len(cols))
		row := make([]interface{}, len(cols))
		dest := make([]interface{}, len(cols))

		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		if err := rows.Scan(dest...); err != nil {
			return "", err
		}

		for i, raw := range rawResult {
			row[i] = string(raw)
		}
		table.AppendRow(row)
	}
	return table.Render(), nil
}
