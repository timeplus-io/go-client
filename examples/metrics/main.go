package main

import (
	"fmt"

	timeplus "github.com/timeplus-io/go-client/client"
	"github.com/timeplus-io/go-client/metrics"
)

func main() {
	timeplusClient := timeplus.NewCient("http://localhost:8000", "")
	_, err := metrics.NewMetrics("cpu1", []string{"a", "x", "g"}, []string{"value"}, timeplusClient)
	if err != nil {
		fmt.Printf("failed to create metric, %s\n", err)
	}

	_, err = metrics.GetMetrics("cpu1", timeplusClient)
	if err != nil {
		fmt.Printf("failed to get metric, %s\n", err)
	}

}
