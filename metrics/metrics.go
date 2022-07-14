package metrics

import (
	"fmt"
	"sync"
	"time"

	timeplus "github.com/timeplus-io/go-client/client"
)

const DefaultTTL = "to_datetime(_tp_time) + INTERVAL 30 DAY"
const DefaultLogStoreRetentionBytes = 604800000
const DefaultLogStoreRetentionMS = 1342177280

type Metrics struct {
	name       string
	tagNames   []string
	valueNames []string

	timeplusClient *timeplus.TimeplusClient
	observations   []*Observation
	lock           sync.Mutex
	interval       time.Duration
	streamDef      timeplus.StreamDef
	streamCols     []string
}

type Observation struct {
	namespace string
	subsystem string
	tags      []string
	values    []float64
	extraTags map[string]interface{}
}

func NewMetrics(name string, tags []string, values []string, timeplusClient *timeplus.TimeplusClient) (*Metrics, error) {
	m := &Metrics{
		name:           name,
		tagNames:       tags,
		valueNames:     values,
		timeplusClient: timeplusClient,
		observations:   make([]*Observation, 0),
		lock:           sync.Mutex{},
		interval:       1 * time.Second,
	}
	if err := m.init(); err != nil {
		return nil, err
	}
	go m.start()
	return m, nil
}

func (m *Metrics) init() error {
	streamDef := timeplus.StreamDef{
		Name: m.name,
		Columns: []timeplus.ColumnDef{
			{
				Name: "namepsace",
				Type: "string",
			},
			{
				Name: "subsystem",
				Type: "string",
			},
			{
				Name: "tags",
				Type: "json",
			},
		},
		TTLExpression:          DefaultTTL,
		LogStoreRetentionBytes: DefaultLogStoreRetentionBytes,
		LogStoreRetentionMS:    DefaultLogStoreRetentionMS,
	}

	for _, name := range m.tagNames {
		col := timeplus.ColumnDef{
			Name: name,
			Type: "string",
		}
		streamDef.Columns = append(streamDef.Columns, col)
	}

	for _, value := range m.valueNames {
		col := timeplus.ColumnDef{
			Name: value,
			Type: "float64",
		}
		streamDef.Columns = append(streamDef.Columns, col)
	}

	m.streamDef = streamDef
	m.streamCols = m.getCols()
	return m.timeplusClient.CreateStream(streamDef)
}

func (m *Metrics) start() {
	for {
		obs := m.getObservations()
		if len(obs) > 0 {
			payload := m.toIngestPayload(obs)
			if err := m.timeplusClient.InsertData(payload); err != nil {
				fmt.Printf("failed to ingest metrics data %s", err)
			}
		}
		time.Sleep(m.interval)
	}
}

func (m *Metrics) getObservations() []*Observation {
	m.lock.Lock()
	defer m.lock.Unlock()

	obs := m.observations
	m.observations = make([]*Observation, 0)
	return obs
}

func (m *Metrics) getCols() []string {
	cols := make([]string, len(m.streamDef.Columns))
	for index, col := range m.streamDef.Columns {
		cols[index] = col.Name
	}
	return cols
}

func (m *Metrics) toIngestPayload(obs []*Observation) timeplus.IngestPayload {
	payload := timeplus.IngestPayload{
		Stream: m.name,
		Data: timeplus.IngestData{
			Columns: m.streamCols,
			Data:    make([][]any, len(obs)),
		},
	}

	for index, ob := range obs {
		payload.Data.Data[index] = m.toIngestRow(ob)
	}
	return payload
}

func (m *Metrics) toIngestRow(ob *Observation) []any {
	// TODO: need to make sure the ob match the schema, not extra tags or values
	row := make([]any, 0)
	row = append(row, ob.namespace)
	row = append(row, ob.subsystem)
	row = append(row, ob.extraTags)

	for _, tag := range ob.tags {
		row = append(row, tag)
	}

	for _, value := range ob.values {
		row = append(row, value)
	}
	return row
}

func (m *Metrics) Observe(namepsace string, subsystem string, tags []string, values []float64, extraTags map[string]interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()
	ob := &Observation{
		namespace: namepsace,
		subsystem: subsystem,
		tags:      tags,
		values:    values,
		extraTags: extraTags,
	}
	m.observations = append(m.observations, ob)
}