package main

import (
	"fmt"

	timeplus "github.com/timeplus-io/go-client/client"
	"github.com/timeplus-io/go-client/metrics"
)

func main() {
	timeplusClient := timeplus.NewCient("http://localhost:8000", "")
	_, err := metrics.NewMetrics("cpu", []string{}, []string{"value"}, timeplusClient)
	if err != nil {
		fmt.Printf("failed to create metric")
	}
}
