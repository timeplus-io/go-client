package main

import (
	"fmt"
	"time"

	timeplus "github.com/timeplus-io/go-client/client"
	"github.com/timeplus-io/go-client/metrics"
)

func main() {
	timeplusClient := timeplus.NewCient("http://localhost:8000", "")
	var m *metrics.Metrics
	m, err := metrics.CreateMetrics("cpu", []string{"a", "x", "g"}, []string{"value"}, timeplusClient)
	if err != nil {
		fmt.Printf("failed to create metric, %s\n", err)
		m, err = metrics.GetMetrics("cpu", timeplusClient)
		if err != nil {
			fmt.Printf("failed to get metric, %s\n", err)
			return
		}
	} else {
		time.Sleep(3 * time.Second)
	}

	m.Observe("timeplus", "test", []any{"xxx", "xxx", nil}, []any{128.9}, nil)
	m.Observe("timeplus", "test", []any{"xxx", "xxx", "xxx"}, []any{12.3}, map[string]interface{}{"a": "b"})
	m.Observe("timeplus", "x1", []any{"xxx", "xxx", "xxx"}, []any{0}, nil)
	m.Observe("timeplus", "x1", []any{"xxx", "xxx", "xxx"}, []any{nil}, nil)

	time.Sleep(3 * time.Second)
}
