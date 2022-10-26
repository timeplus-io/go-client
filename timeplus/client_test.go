package timeplus_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/reactivex/rxgo/v2"
	"github.com/timeplus-io/go-client/timeplus"
)

func TestClient(t *testing.T) {
	timeplusAddress := os.Getenv("TIMEPLUS_ADDRESS")
	timeplusApiKey := os.Getenv("TIMEPLUS_API_KEY")
	timeplusTenant := os.Getenv("TIMEPLUS_TENANT")

	timeplusClient := timeplus.NewCient(timeplusAddress, timeplusTenant, timeplusApiKey)
	streamResult, queryResult, err := timeplusClient.QueryStream("select * from car_live_data")

	if err != nil {
		fmt.Printf("failed to run query, %s\n", err)
		return
	}

	fmt.Printf("query result header is, %v\n", queryResult.Result.Header)

	bufferStream := streamResult.Take(2)
	disposed := bufferStream.ForEach(func(v interface{}) {
		event := v.([]interface{})
		fmt.Printf("got one event %v\n", event)
		for _, cell := range event[0 : len(event)-2] {
			fmt.Printf("got one cell %v:%T\n", cell, cell)
		}
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
