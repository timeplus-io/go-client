package timeplus_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/timeplus-io/go-client/timeplus"
)

func TestLow(t *testing.T) {
	timeplusAddress := os.Getenv("TIMEPLUS_LOW_LEVEL_ADDRESS")
	timeplusClient := timeplus.NewLowLevelCient(timeplusAddress)

	data := &timeplus.IngestPayload{
		Data: timeplus.IngestData{
			Columns: []string{"f1"},
			Data:    [][]any{{1}, {2}},
		},
		Stream: "test",
	}

	if err := timeplusClient.InsertData(data); err != nil {
		fmt.Printf("failed to ingest data %s\n", err)
	} else {
		fmt.Printf("succeed to ingest data\n")
	}

}
