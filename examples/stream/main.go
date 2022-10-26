package main

import (
	"fmt"
	"os"

	"github.com/timeplus-io/go-client/timeplus"
)

func main() {
	timeplusAddress := os.Getenv("TIMEPLUS_ADDRESS")
	timeplusApiKey := os.Getenv("TIMEPLUS_API_KEY")
	timeplusTenant := os.Getenv("TIMEPLUS_TENANT")

	timeplusClient := timeplus.NewCient(timeplusAddress, timeplusTenant, timeplusApiKey)
	if streams, err := timeplusClient.ListStream(); err != nil {
		fmt.Printf("failed to list existing streams %s\n", err)
	} else {
		fmt.Printf("find %d streams\n", len(streams))
	}
}
