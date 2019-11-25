package tpch

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
)

type precision int

const (
	str precision = iota
	sum
	avg
	cnt
	num
	rat
)

var queryColPrecisions = map[string][]precision{
	// Comment 4: In cases where validation output data is from the aggregate SUM(l_quantity) (e.g. queries 1 and 18),
	// the precision for this validation output data must exactly match the query validation data.
	"q1":  {str, str, str, sum, sum, sum, avg, avg, avg, cnt},
	"q2":  {num, str, str, str, str, str, str, str},
	"q3":  {str, sum, str, str},
	"q4":  {str, cnt},
	"q5":  {str, sum},
	"q6":  {sum},
	"q7":  {str, str, str, sum},
	"q8":  {str, rat},
	"q9":  {str, str, sum},
	"q10": {str, str, sum, num, str, str, str, str},
	"q11": {str, sum},
	// Comment 2: In cases where validation output data resembles a row count operation by summing up 0 and 1 using a
	// SUM aggregate (e.g. query 12), the precision for this validation output data must adhere to bullet a) above.
	"q12": {str, cnt, cnt},
	"q13": {cnt, cnt},
	"q14": {rat},
	// Comment 3: In cases were validation output data is selected from views without any further computation (e.g. total
	// revenue in Query 15), the precision for this validation output data must adhere to bullet c) above.
	"q15": {str, str, str, str, sum},
	"q16": {str, str, num, cnt},
	"q17": {avg},
	// Comment 4: In cases where validation output data is from the aggregate SUM(l_quantity) (e.g. queries 1 and 18),
	// the precision for this validation output data must exactly match the query validation data.
	"q18": {str, str, str, str, num, str},
	"q19": {sum},
	"q20": {str, str},
	"q21": {str, cnt},
	"q22": {num, cnt, sum},
}

func (w Workloader) checkQueryResult(queryName string, rows *sql.Rows) error {
	defer rows.Close()
	var got [][]string

	cols, err := rows.Columns()
	if err != nil {
		return nil
	}

	for rows.Next() {
		rawResult := make([][]byte, len(cols))
		row := make([]string, len(cols))
		dest := make([]interface{}, len(cols))

		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

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

	return checkOutput(queryColPrecisions[queryName], ans[queryName], got)
}

func checkOutput(colPrecisions []precision, expect [][]string, got [][]string) (ret error) {
	if len(expect) != len(got) {
		return fmt.Errorf("expect %d rows, got %d rows", len(expect), len(got))
	}

	for i, row := range got {
		for j, column := range row {
			expectStr := expect[i][j]
			ret = fmt.Errorf("expect %s at row %d column %d, got %s", expectStr, i, j, column)

			// 2.1.3.5
			switch colPrecisions[j] {
			case cnt:
				// For singleton column values and results from COUNT aggregates, the values must exactly match the query
				// validation output data.
				fallthrough
			case num:
				fallthrough
			case str:
				if expectStr != column {
					return
				}
				continue
			}

			expectFloat, err := strconv.ParseFloat(expectStr, 64)
			if err != nil {
				return
			}
			gotFloat, err := strconv.ParseFloat(column, 64)
			if err != nil {
				return
			}

			switch colPrecisions[j] {
			case sum:
				// For results from SUM aggregates, the resulting values must be within $100 of the query validation output
				// data
				if math.Abs(expectFloat-gotFloat) > 100.0 {
					return
				}
			case avg:
				// For results from AVG aggregates, the resulting values r must be within 1% of the query validation output
				// data when rounded to the nearest 1/100th. That is, 0.99*v<=round(r,2)<=1.01*v.
				fallthrough
			case rat:
				// For ratios, results r must be within 1% of the query validation output data v when rounded to the nearest
				// 1/100th. That is, 0.99*v<=round(r,2)<=1.01*v
				if math.Abs(math.Round(gotFloat*1000)/1000-math.Round(expectFloat*1000)/1000) > 0.01 {
					return
				}
			default:
				panic("unreachable")
			}
		}
	}

	return nil
}
