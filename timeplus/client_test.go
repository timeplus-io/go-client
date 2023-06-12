package timeplus_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/timeplus-io/go-client/timeplus"
)

func TestClient(t *testing.T) {
	timeplusAddress := os.Getenv("TIMEPLUS_ADDRESS")
	timeplusApiKey := os.Getenv("TIMEPLUS_API_KEY")
	timeplusTenant := os.Getenv("TIMEPLUS_TENANT")

	timeplusClient := timeplus.NewCient(timeplusAddress, timeplusTenant, timeplusApiKey)
	stream, queryResult, err := timeplusClient.QueryStream("select * from car_live_data", 100, 128)

	if err != nil {
		fmt.Printf("Query Failed! %s\n", err)
	}

	fmt.Printf("query result header is, %v\n", ((*queryResult)["result"]).(map[string]any)["header"])

	bufferStream := stream.Take(1)
	disposed := bufferStream.ForEach(func(v interface{}) {
		event := v.(*timeplus.DataEvent)
		fmt.Printf("got one event %v\n", event)
	}, func(err error) {
		fmt.Printf("failed to query %s", err)
	}, func() {

	})

	<-disposed
}
