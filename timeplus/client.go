package timeplus

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/reactivex/rxgo/v2"

	"github.com/timeplus-io/go-client/utils"
)

const TimeFormat = "2006-01-02 15:04:05.000"
const APIVersion = "v1beta1"
const APIV2Version = "v1beta2"

type QueryEvent map[string]any
type MetricsEvent map[string]any
type DataEvent [][]any

type ServerSideEvent struct {
	EventType   string
	QueryData   *QueryEvent
	MetricsData *MetricsEvent
	Data        *DataEvent
}

type ColumnDef struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Default string `json:"default"`
}

type StreamDef struct {
	Name                   string      `json:"name"`
	Columns                []ColumnDef `json:"columns"`
	EventTimeColumn        string      `json:"event_time_column,omitempty"`
	EventTimeZone          string      `json:"event_time_timezone,omitempty"`
	TTLExpression          string      `json:"ttl_expression,omitempty"`
	LogStoreRetentionBytes int         `json:"logstore_retention_bytes,omitempty"`
	LogStoreRetentionMS    int         `json:"logstore_retention_ms,omitempty"`
}

type View struct {
	Name         string `json:"name"`
	Query        string `json:"query"`
	Materialized bool   `json:"materialized,omitempty"`
}

type IngestData struct {
	Columns []string `json:"columns"`
	Data    [][]any  `json:"data"`
}

type IngestPayload struct {
	Data   IngestData `json:"data"`
	Stream string     `json:"stream"`
}

type Query struct {
	SQL         string         `json:"sql"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Tags        []string       `json:"tags"`
	Policy      BatchingPolicy `json:"batching_policy,omitempty"`
}

type BatchingPolicy struct {
	Count  int `json:"count,omitempty"`
	TimeMS int `json:"time_ms,omitempty"`
}

type QueryInfo struct {
	ID           string      `json:"id"`
	Name         string      `json:"name"`
	SQL          string      `json:"sql"`
	Description  string      `json:"description"`
	Tags         []string    `json:"tags"`
	Stat         QueryStat   `json:"stat"`
	StartTime    int64       `json:"start_time"`
	EndTime      int64       `json:"end_time"`
	Duration     int64       `json:"duration"`
	ResponseTime int64       `json:"response_time"`
	Status       string      `json:"status"`
	Message      string      `json:"message"`
	Result       QueryResult `json:"result"`
}

type SQLRequest struct {
	SQL     string `json:"sql"`
	Timeout int    `json:"timeout"`
}

type QueryResult struct {
	Header []ColumnDef     `json:"header"`
	Data   [][]interface{} `json:"data"`
}

type QueryStat struct {
	Count      int            `json:"count"`
	Latency    LatencyStat    `json:"latency"`
	Throughput ThroughputStat `json:"throughput"`
}

type LatencyStat struct {
	Min    float64   `json:"min"`
	Max    float64   `json:"max"`
	Sum    float64   `json:"sum"`
	Avg    float64   `json:"avg"`
	Latest []float64 `json:"latest"`
}

type ThroughputStat struct {
	Value float32 `json:"value"`
}

type TimeplusClient struct {
	address string
	apikey  string
	tenant  string
	client  *http.Client
}

func NewCient(address string, tenant string, apikey string) *TimeplusClient {
	return &TimeplusClient{
		address: address,
		apikey:  apikey,
		tenant:  tenant,
		client:  utils.NewDefaultHttpClient(),
	}
}

func NewCientWithHttpConfig(address string, tenant string, apikey string, config *utils.HTTPClientConfig) *TimeplusClient {
	return &TimeplusClient{
		address: address,
		apikey:  apikey,
		tenant:  tenant,
		client:  utils.NewHttpClient(*config),
	}
}

func (s *TimeplusClient) baseUrl() string {
	if len(s.tenant) == 0 {
		return fmt.Sprintf("%s/api/%s", s.address, APIVersion)
	} else {
		return fmt.Sprintf("%s/%s/api/%s", s.address, s.tenant, APIVersion)
	}
}

func (s *TimeplusClient) baseUrlV2() string {
	if len(s.tenant) == 0 {
		return fmt.Sprintf("%s/api/%s", s.address, APIV2Version)
	} else {
		return fmt.Sprintf("%s/%s/api/%s", s.address, s.tenant, APIV2Version)
	}
}

func (s *TimeplusClient) CreateStream(streamDef StreamDef) error {
	url := fmt.Sprintf("%s/streams", s.baseUrl())
	_, _, err := utils.HttpRequestWithAPIKey(http.MethodPost, url, streamDef, s.client, s.apikey)
	if err != nil {
		return fmt.Errorf("failed to create stream %s: %w", streamDef.Name, err)
	}
	return nil
}

func (s *TimeplusClient) DeleteStream(streamName string) error {
	url := fmt.Sprintf("%s/streams/%s", s.baseUrl(), streamName)
	_, _, err := utils.HttpRequestWithAPIKey(http.MethodDelete, url, nil, s.client, s.apikey)
	if err != nil {
		return fmt.Errorf("failed to delete stream %s: %w", streamName, err)
	}
	return nil
}

func (s *TimeplusClient) ExistStream(name string) bool {
	streams, err := s.ListStream()
	if err != nil {
		return false
	}

	for _, s := range streams {
		if s.Name == name {
			return true
		}
	}

	return false
}

func (s *TimeplusClient) GetStream(name string) (*StreamDef, error) {
	streams, err := s.ListStream()
	if err != nil {
		return nil, err
	}

	for _, s := range streams {
		if s.Name == name {
			return &s, nil
		}
	}

	return nil, fmt.Errorf("stream %s not found", name)
}

func (s *TimeplusClient) ListStream() ([]StreamDef, error) {
	url := fmt.Sprintf("%s/streams", s.baseUrl())
	_, respBody, err := utils.HttpRequestWithAPIKey(http.MethodGet, url, nil, s.client, s.apikey)
	if err != nil {
		return nil, fmt.Errorf("failed to list stream : %w", err)
	}

	var payload []StreamDef
	json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&payload)

	return payload, nil
}

func (s *TimeplusClient) CreateView(view View) error {
	url := fmt.Sprintf("%s/views", s.baseUrl())
	_, _, err := utils.HttpRequestWithAPIKey(http.MethodPost, url, view, s.client, s.apikey)
	if err != nil {
		return fmt.Errorf("failed to create view %s: %w", view.Name, err)
	}
	return nil
}

func (s *TimeplusClient) ListView() ([]View, error) {
	url := fmt.Sprintf("%s/views", s.baseUrl())
	_, respBody, err := utils.HttpRequestWithAPIKey(http.MethodGet, url, nil, s.client, s.apikey)
	if err != nil {
		return nil, fmt.Errorf("failed to list views : %w", err)
	}

	var payload []View
	json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&payload)

	return payload, nil
}

func (s *TimeplusClient) ExistView(name string) bool {
	views, err := s.ListView()
	if err != nil {
		return false
	}

	for _, v := range views {
		if v.Name == name {
			return true
		}
	}

	return false
}

func (s *TimeplusClient) InsertData(data *IngestPayload) error {
	url := fmt.Sprintf("%s/streams/%s/ingest", s.baseUrl(), data.Stream)
	_, _, err := utils.HttpRequestWithAPIKey(http.MethodPost, url, data.Data, s.client, s.apikey)
	if err != nil {
		return fmt.Errorf("failed to ingest data into stream %s: %w", data.Stream, err)
	}
	return nil
}

func (s *TimeplusClient) QueryStream(sql string) (rxgo.Observable, *QueryInfo, error) {
	query := Query{
		SQL:         sql,
		Name:        "",
		Description: "",
		Tags:        []string{},
	}

	createQueryUrl := fmt.Sprintf("%s/queries", s.baseUrl())
	_, respBody, err := utils.HttpRequestWithAPIKey(http.MethodPost, createQueryUrl, query, s.client, s.apikey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create query : %w", err)
	}

	var queryResult QueryInfo
	json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&queryResult)

	var wsUrl string
	if len(s.tenant) == 0 {
		wsUrl = fmt.Sprintf("%s/ws/queries/%s", s.address, queryResult.ID)
	} else {
		wsUrl = fmt.Sprintf("%s/%s/ws/queries/%s", s.address, s.tenant, queryResult.ID)
	}
	wsUrl = strings.Replace(wsUrl, "http", "ws", 1)

	requestHeader := http.Header{}
	requestHeader.Set("X-Api-Key", s.apikey)

	c, _, err := websocket.DefaultDialer.Dial(wsUrl, requestHeader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to websocket %s : %w", wsUrl, err)
	}

	streamChannel := make(chan rxgo.Item)
	resultStream := rxgo.FromChannel(streamChannel, rxgo.WithPublishStrategy())

	go func() {
		defer close(streamChannel)
		defer c.Close()

		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				return
			}
			var messagePayload []interface{}
			json.NewDecoder(bytes.NewBuffer(message)).Decode(&messagePayload)
			//event := toEvent(queryResult.Result.Header, messagePayload)
			event := messagePayload
			streamChannel <- rxgo.Of(event)
		}
	}()
	return resultStream, &queryResult, nil
}

func (s *TimeplusClient) QueryV2Stream(sql string, count int, bufferTime int) (rxgo.Observable, error) {
	query := Query{
		SQL:         sql,
		Name:        "",
		Description: "",
		Tags:        []string{},
		Policy: BatchingPolicy{
			Count:  count,
			TimeMS: bufferTime,
		},
	}

	createQueryUrl := fmt.Sprintf("%s/queries", s.baseUrlV2())
	config := utils.NewDefaultHTTPClientConfig()
	res, err := utils.SSEHttpRequestWithAPIKey(http.MethodPost, createQueryUrl, query, config, s.apikey)
	if err != nil {
		return nil, fmt.Errorf("failed to create query : %w", err)
	}

	scanner := bufio.NewScanner(res.Body)
	ch := make(chan rxgo.Item)
	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if len(line) == 0 {
				continue
			}

			colonIndex := strings.Index(line, ":")
			eventField := strings.TrimSpace(line[0:colonIndex])
			eventData := strings.TrimSpace(line[colonIndex+1:])

			if eventField == "event" {
				scanner.Scan()
				dataLine := scanner.Text()
				colonIndex := strings.Index(dataLine, ":")
				eventContentData := dataLine[colonIndex+1:]
				if eventData == "query" {
					var m QueryEvent
					err := json.Unmarshal([]byte(eventContentData), &m)
					if err != nil {
						ch <- rxgo.Error(fmt.Errorf("invalide sse response,%s, %s", err, eventContentData))
					}

					event := &ServerSideEvent{
						EventType: "query",
						QueryData: &m,
					}
					ch <- rxgo.Of(event)
				} else if eventData == "metrics" {
					var m MetricsEvent
					err := json.Unmarshal([]byte(eventContentData), &m)
					if err != nil {
						ch <- rxgo.Error(fmt.Errorf("invalide sse response,%s, %s", err, eventContentData))
					}
					event := &ServerSideEvent{
						EventType:   "metrics",
						MetricsData: &m,
					}
					ch <- rxgo.Of(event)
				}
			} else if eventField == "data" {

				var m DataEvent
				err := json.Unmarshal([]byte(eventData), &m)
				if err != nil {
					ch <- rxgo.Error(fmt.Errorf("invalide sse response, %s", line))
				}
				event := &ServerSideEvent{
					EventType: "data",
					Data:      &m,
				}
				ch <- rxgo.Of(event)
			}
		}

		if err := scanner.Err(); err != nil {
			ch <- rxgo.Error(err)
		}
		close(ch)
		res.Body.Close()
	}()

	observable := rxgo.FromChannel(ch, rxgo.WithPublishStrategy())
	return observable, nil
}

func (e *ServerSideEvent) GetQuery() (*QueryEvent, error) {
	if e.EventType != "query" {
		return nil, fmt.Errorf("the event is not query")
	}

	return e.QueryData, nil
}

func (e *ServerSideEvent) GetMetrics() (*MetricsEvent, error) {
	if e.EventType != "metrics" {
		return nil, fmt.Errorf("the event is not metrics")
	}

	return e.MetricsData, nil
}

func (e *ServerSideEvent) GetData() (*DataEvent, error) {
	if e.EventType != "data" {
		return nil, fmt.Errorf("the event is not data")
	}

	return e.Data, nil
}
