package client_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/reactivex/rxgo/v2"
	"github.com/timeplus-io/go-client/client"
)

func TestClient(t *testing.T) {
	timeplusAddress := os.Getenv("TIMEPLUS_ADDRESS")
	timeplusApiKey := os.Getenv("TIMEPLUS_API_KEY")
	timeplusTenant := os.Getenv("TIMEPLUS_TENANT")

	timeplusClient := client.NewCient(timeplusAddress, timeplusTenant, timeplusApiKey)
	streamResult, err := timeplusClient.QueryStream("select 1")

	if err != nil {
		fmt.Printf("failed to run query, %s\n", err)
		return
	}

	disposed := streamResult.ForEach(func(v interface{}) {
		event := v.(map[string]interface{})
		fmt.Printf("got one event %v", event)

	}, func(err error) {
		fmt.Printf("failed to query %s", err)
	}, func() {

	})

	_, cancel := streamResult.Connect(context.Background())

	go func(cancel rxgo.Disposable) {
		time.Sleep(4 * time.Second)
		cancel()
	}(cancel)

	<-disposed
}
