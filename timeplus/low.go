package timeplus

import (
	"fmt"
	"net/http"

	"github.com/timeplus-io/go-client/utils"
)

type TimeplusLowLevelClient struct {
	address string
	client  *http.Client
}

func NewLowLevelCient(address string) *TimeplusLowLevelClient {
	return &TimeplusLowLevelClient{
		address: address,
		client:  utils.NewDefaultHttpClient(),
	}
}

func (s *TimeplusLowLevelClient) baseUrl() string {
	return fmt.Sprintf("%s/%s", s.address, "proton/v1")
}

func (s *TimeplusLowLevelClient) InsertData(data *IngestPayload) error {
	url := fmt.Sprintf("%s/%s/%s", s.baseUrl(), "ingest/streams", data.Stream)
	_, _, err := utils.HttpRequest(http.MethodPost, url, data.Data, s.client)
	if err != nil {
		return fmt.Errorf("failed to ingest data into stream %s: %w", data.Stream, err)
	}
	return nil
}
