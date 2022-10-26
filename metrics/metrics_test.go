package metrics_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/timeplus-io/go-client/metrics"
	"github.com/timeplus-io/go-client/timeplus"
)

func TestMetric(t *testing.T) {
	timeplusAddress := os.Getenv("TIMEPLUS_ADDRESS")
	timeplusApiKey := os.Getenv("TIMEPLUS_API_KEY")
	timeplusTenant := os.Getenv("TIMEPLUS_TENANT")

	timeplusClient := timeplus.NewCient(timeplusAddress, timeplusTenant, timeplusApiKey)
	var m *metrics.Metrics
	m, err := metrics.CreateMetrics("cpu", []string{"a", "x", "g"}, []string{"value"}, timeplusClient, 1*time.Second)
	if err != nil {
		fmt.Printf("failed to create metric, %s\n", err)
		m, err = metrics.GetMetrics("cpu", timeplusClient, 1*time.Second)
		if err != nil {
			fmt.Printf("failed to get metric, %s\n", err)
			return
		}
	} else {
		time.Sleep(3 * time.Second)
	}

	if err = m.Observe("timeplus", "test", []any{"xxx", "xxx", nil}, []any{128.9}, nil); err != nil {
		fmt.Printf("failed to observer %s\n", err)
	}
	m.Observe("timeplus", "test", []any{"xxx", "xxx", "xxx"}, []any{12.3}, map[string]interface{}{"a": "b"})
	m.Observe("timeplus", "x1", []any{"xxx", "xxx", "xxx"}, []any{0}, nil)
	m.Observe("timeplus", "x1", []any{"xxx", "xxx", "xxx"}, []any{nil}, nil)

	time.Sleep(3 * time.Second)
}
