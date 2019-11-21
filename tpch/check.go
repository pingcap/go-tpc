package tpch

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
)

func (w Workloader) checkQueryResult(queryName string, rows *sql.Rows) error {
	defer rows.Close()
	var got [][]string

	cols, err := rows.Columns()
	if err != nil {
		return nil
	}

	rawResult := make([][]byte, len(cols))
	row := make([]string, len(cols))
	dest := make([]interface{}, len(cols))

	for i := range rawResult {
		dest[i] = &rawResult[i]
	}

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("scan %s failed %v", queryName, err)
		}

		for i, raw := range rawResult {
			if raw == nil {
				row[i] = "\\N"
			} else {
				row[i] = string(raw)
			}
		}
		got = append(got, row)
	}

	return checkOutput(ans[queryName], got)
}

func checkOutput(expect [][]string, got [][]string) error {
	if len(expect) != len(got) {
		return fmt.Errorf("expect %d rows, got %d rows", len(expect), len(got))
	}

	for i, row := range got {
		for j, column := range row {
			expectStr := expect[i][j]
			if expectStr == column {
				continue
			}
			expectFloat, err := strconv.ParseFloat(expectStr, 64)
			if err != nil {
				fmt.Errorf("expect %s at row %d column %d, got %s", expectStr, i, j, column)
			}
			gotFloat, err := strconv.ParseFloat(column, 64)
			if err != nil {
				fmt.Errorf("expect %s at row %d column %d, got %s", expectStr, i, j, column)
			}

			// 2.1.3.5
			if 0.99*expectFloat <= math.Round(gotFloat) && math.Round(gotFloat) <= 1.01*expectFloat {
				continue
			}
			fmt.Errorf("expect %s at row %d column %d, got %s", expectStr, i, j, column)
		}
	}

	return nil
}
