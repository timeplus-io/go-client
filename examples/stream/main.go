package main

import (
	"fmt"

	"github.com/timeplus-io/go-client/client"
)

func main() {
	timeplusClient := client.NewCient("http://localhost:8000", "", "")
	if streams, err := timeplusClient.ListStream(); err != nil {
		fmt.Printf("failed to list existing streams\n")
	} else {
		fmt.Printf("find %d streams\n", len(streams))
	}
}
