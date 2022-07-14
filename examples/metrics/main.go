package main

import (
	"fmt"
	"time"

	timeplus "github.com/timeplus-io/go-client/client"
	"github.com/timeplus-io/go-client/metrics"
)

func main() {
	timeplusClient := timeplus.NewCient("http://localhost:8000", "")
	m, err := metrics.NewMetrics("cpu1", []string{"a", "x", "g"}, []string{"value"}, timeplusClient)
	if err != nil {
		fmt.Printf("failed to create metric, %s\n", err)
		m, err = metrics.GetMetrics("cpu1", timeplusClient)
		if err != nil {
			fmt.Printf("failed to get metric, %s\n", err)
			return
		}
	}

	m.Observe("timeplus", "test", []string{"xxx", "xxx", "xxx"}, []float64{12.3}, map[string]interface{}{"a": "b"})
	m.Observe("timeplus", "test", []string{"xxx", "xxx", "xxx"}, []float64{128.9}, nil)

	time.Sleep(3 * time.Second)
}
