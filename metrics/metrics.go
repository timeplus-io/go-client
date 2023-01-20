package metrics

import (
	"fmt"
	"sync"
	"time"

	"github.com/timeplus-io/go-client/timeplus"
)

const DefaultTTL = "to_datetime(_tp_time) + INTERVAL 30 DAY"
const DefaultLogStoreRetentionBytes = 604800000
const DefaultLogStoreRetentionMS = 1342177280

const NumberOfInternalFields = 1

type Metrics struct {
	name       string
	tagNames   []string
	valueNames []string

	timeplusClient *timeplus.TimeplusClient
	streamName     string
	observations   []*Observation
	lock           sync.Mutex
	interval       time.Duration
	streamDef      timeplus.StreamDef
	streamCols     []string
}

type Observation struct {
	timestamp string
	namespace string
	subsystem string
	tags      []any
	values    []any
	extraTags map[string]interface{}
}

func CreateMetrics(name string, tags []string, values []string, timeplusClient *timeplus.TimeplusClient, pushInterval time.Duration) (*Metrics, error) {
	m := &Metrics{
		name:           name,
		tagNames:       tags,
		valueNames:     values,
		timeplusClient: timeplusClient,
		observations:   make([]*Observation, 0),
		lock:           sync.Mutex{},
		interval:       pushInterval,
	}
	if err := m.create(); err != nil {
		return nil, err
	}
	go m.start()
	return m, nil
}

func GetMetrics(name string, timeplusClient *timeplus.TimeplusClient, pushInterval time.Duration) (*Metrics, error) {
	m := &Metrics{
		name:           name,
		timeplusClient: timeplusClient,
		observations:   make([]*Observation, 0),
		lock:           sync.Mutex{},
		interval:       pushInterval,
	}
	if err := m.get(); err != nil {
		return nil, err
	}
	go m.start()
	return m, nil
}

func NewMetrics(name string, tags []string, values []string, timeplusClient *timeplus.TimeplusClient, pushInterval time.Duration) (*Metrics, error) {
	var m *Metrics
	m, err := GetMetrics(name, timeplusClient, pushInterval)
	if err != nil {
		if m, err := CreateMetrics(name, tags, values, timeplusClient, pushInterval); err != nil {
			return nil, err
		} else {
			return m, nil
		}
	}
	return m, nil
}

func (m *Metrics) createMetricStream() error {
	streamDef := timeplus.StreamDef{
		Name: m.streamName,
		Columns: []timeplus.ColumnDef{
			{
				Name: "timestamp",
				Type: "string",
			},
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
		EventTimeColumn:        "to_datetime64(timestamp,9)",
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
	fmt.Printf("the stream cols is %v\n", m.streamCols)
	return m.timeplusClient.CreateStream(streamDef)
}

func (m *Metrics) create() error {
	m.streamName = fmt.Sprintf("_tp_metric_%s", m.name)
	if m.timeplusClient.ExistStream(m.streamName) {
		return fmt.Errorf("metrics stream already exist")
	} else {
		return m.createMetricStream()
	}
}

func (m *Metrics) getMetricStream() error {
	stream, err := m.timeplusClient.GetStream(m.streamName)
	if err != nil {
		return err
	}

	m.streamDef = *stream
	allCols := m.getCols()

	m.streamCols = allCols[0 : len(allCols)-NumberOfInternalFields]
	m.tagNames = m.getTagNames()
	m.valueNames = m.getValueNames()
	fmt.Printf("the stream cols is %v\n", m.streamCols)
	return nil
}

func (m *Metrics) getTagNames() []string {
	result := make([]string, 0)
	cols := m.streamDef.Columns[4 : len(m.streamDef.Columns)-NumberOfInternalFields]
	for _, col := range cols {
		if col.Type == "string" {
			result = append(result, col.Name)
		}
	}
	return result
}

func (m *Metrics) getValueNames() []string {
	result := make([]string, 0)
	cols := m.streamDef.Columns[4 : len(m.streamDef.Columns)-NumberOfInternalFields]
	for _, col := range cols {
		if col.Type == "float64" {
			result = append(result, col.Name)
		}
	}
	return result
}

func (m *Metrics) get() error {
	m.streamName = fmt.Sprintf("_tp_metric_%s", m.name)
	if !m.timeplusClient.ExistStream(m.streamName) {
		return fmt.Errorf("metrics stream does not exist")
	} else {
		return m.getMetricStream()
	}
}

func (m *Metrics) start() {
	for {
		m.Flush()
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

func (m *Metrics) toIngestPayload(obs []*Observation) *timeplus.IngestPayload {
	payload := &timeplus.IngestPayload{
		Stream: m.streamName,
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
	row := make([]any, 0)
	row = append(row, ob.timestamp)
	row = append(row, ob.namespace)
	row = append(row, ob.subsystem)
	row = append(row, ob.extraTags)
	row = append(row, ob.tags...)
	row = append(row, ob.values...)
	return row
}

// Note, the tags could be nil or string
// Note, the values could be nil or float64
func (m *Metrics) Observe(namepsace string, subsystem string, tags []any, values []any, extraTags map[string]interface{}) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if len(tags) != len(m.tagNames) {
		return fmt.Errorf("the number of tags does not match, should be %d", len(m.tagNames))
	}

	if len(values) != len(m.valueNames) {
		return fmt.Errorf("the number of valus does not match, should be %d", len(m.valueNames))
	}

	// TODO: should validate the tags are string/nil and values are number/nil

	ob := &Observation{
		timestamp: fmt.Sprintf("%d", time.Now().UnixNano()),
		namespace: namepsace,
		subsystem: subsystem,
		tags:      tags,
		values:    values,
		extraTags: extraTags,
	}
	m.observations = append(m.observations, ob)
	return nil
}

func (m *Metrics) Flush() {
	obs := m.getObservations()
	if len(obs) > 0 {
		payload := m.toIngestPayload(obs)
		if err := m.timeplusClient.InsertData(payload); err != nil {
			fmt.Printf("failed to ingest metrics data %s", err)
		}
	}
}
