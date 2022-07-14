package client_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/reactivex/rxgo/v2"
	"github.com/timeplus-io/go-client/client"
)

func TestMetric(t *testing.T) {
	timeplusClient := client.NewCient("https://latest.timeplus.io", "")
	streamResult, err := timeplusClient.QueryStream("select * from car_live_data")

	if err != nil {
		fmt.Printf("failed to run query, %s\n", err)
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
