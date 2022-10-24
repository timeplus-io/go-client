package client_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/timeplus-io/go-client/client"
)

func TestLow(t *testing.T) {
	timeplusAddress := os.Getenv("TIMEPLUS_LOW_LEVEL_ADDRESS")
	timeplusClient := client.NewLowLevelCient(timeplusAddress)

	data := client.IngestPayload{
		Data: client.IngestData{
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
