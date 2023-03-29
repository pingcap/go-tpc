package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"
)

// output style
const (
	OutputStylePlain = "plain"
	OutputStyleTable = "table"
	OutputStyleJson  = "json"
)

// This logger is goroutine-safe.
var StdErrLogger *log.Logger

func init() {
	StdErrLogger = log.New(os.Stderr, "", 0)
}

func RenderString(format string, headers []string, values [][]string) {
	if len(values) == 0 {
		return
	}
	if len(headers) == 0 {
		for _, value := range values {
			args := make([]interface{}, len(value))
			for i, v := range value {
				args[i] = v
			}
			fmt.Printf(format, args...)
		}
		return
	}

	buf := new(bytes.Buffer)
	for _, value := range values {
		args := make([]string, len(headers)-2)
		for i, header := range headers[2:] {
			args[i] = header + ": " + value[i+2]
		}
		buf.WriteString(fmt.Sprintf(format, value[0], value[1], strings.Join(args, ", ")))
	}
	fmt.Print(buf.String())
}

func RenderTable(headers []string, values [][]string) {
	if len(values) == 0 {
		return
	}
	tb := tablewriter.NewWriter(os.Stdout)
	tb.SetHeader(headers)
	tb.AppendBulk(values)
	tb.Render()
}

func RenderJson(headers []string, values [][]string) {
	if len(values) == 0 {
		return
	}
	data := make([]map[string]string, 0, len(values))
	for _, value := range values {
		line := make(map[string]string, 0)
		for i, header := range headers {
			line[header] = value[i]
		}
		data = append(data, line)
	}
	outStr, err := json.Marshal(data)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(outStr))
}

func IntToString(i int64) string {
	return fmt.Sprintf("%d", i)
}

func FloatToOneString(f float64) string {
	return fmt.Sprintf("%.1f", f)
}

func FloatToTwoString(f float64) string {
	return fmt.Sprintf("%.2f", f)
}
