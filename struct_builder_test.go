package parquet_test

import (
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/segmentio/parquet"
	"github.com/segmentio/parquet/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	localref "github.com/xitongsys/parquet-go-source/local"
	writerref "github.com/xitongsys/parquet-go/writer"
)

// All the tests in this file use parquet-go and its annotations to generate
// the test parquet files.
// This should be rewritten when we have our own writer.

func TestStructBuilderSimple(t *testing.T) {
	type Record struct {
		Name string `parquet:"name=name, type=UTF8"`
		Foo  int32  `parquet:"name=foo, type=INT_32"`
	}

	expected := []interface{}{
		&Record{Name: "name1", Foo: 1},
		&Record{Name: "name2", Foo: 2},
		&Record{Name: "name3", Foo: 3},
	}

	structBuilderTest(t, new(Record), new(Record), expected)
}

func TestStructBuilderNestedStructs(t *testing.T) {
	type Inner struct {
		Bar int32 `parquet:"name=bar, type=INT_32"`
	}
	type Record struct {
		Foo   int32 `parquet:"name=foo, type=INT_32"`
		Inner Inner `parquet:"name=inner"`
	}

	expected := []interface{}{
		&Record{Foo: 1, Inner: Inner{Bar: 11}},
		&Record{Foo: 2, Inner: Inner{Bar: 22}},
		&Record{Foo: 3, Inner: Inner{Bar: 33}},
	}

	structBuilderTest(t, new(Record), new(Record), expected)
}

func TestStructBuilderTwoNestedStructs(t *testing.T) {
	type Inner struct {
		Bar int32 `parquet:"name=bar, type=INT_32"`
	}
	type InnerTwo struct {
		Bartwo int32 `parquet:"name=bartwo, type=INT_32"`
	}
	type Record struct {
		Foo      int32    `parquet:"name=foo, type=INT_32"`
		Inner    Inner    `parquet:"name=inner"`
		InnerTwo InnerTwo `parquet:"name=inner_two"`
	}

	expected := []interface{}{
		&Record{Foo: 1, Inner: Inner{Bar: 11}, InnerTwo: InnerTwo{Bartwo: 111}},
		&Record{Foo: 2, Inner: Inner{Bar: 22}, InnerTwo: InnerTwo{Bartwo: 222}},
		&Record{Foo: 3, Inner: Inner{Bar: 33}, InnerTwo: InnerTwo{Bartwo: 333}},
	}

	structBuilderTest(t, new(Record), new(Record), expected)
}

func TestStructBuilderList(t *testing.T) {
	type Record struct {
		Foo []int32 `parquet:"name=foo, type=LIST, valuetype=INT32"`
	}

	expected := []interface{}{
		&Record{Foo: []int32{1, 2}},
		&Record{Foo: nil},
		&Record{Foo: []int32{3}},
	}

	structBuilderTest(t, new(Record), new(Record), expected)
}

func TestStructBuilderStructListStruct(t *testing.T) {
	type Tag struct {
		Key   string `parquet:"name=key, type=UTF8"`
		Value string `parquet:"name=value, type=UTF8"`
	}
	type Record struct {
		Tags []Tag `parquet:"name=tags, type=LIST"`
	}

	expected := []interface{}{
		&Record{Tags: []Tag{{Key: "one", Value: "un"}, {Key: "two", Value: "deux"}}},
		&Record{},
		&Record{[]Tag{{Key: "three", Value: "trois"}}},
	}

	structBuilderTest(t, new(Record), new(Record), expected)
}

func TestStructBuilderMap(t *testing.T) {
	type Record struct {
		Map map[string]string `parquet:"name=map, type=MAP, keytype=UTF8, valuetype=UTF8"`
	}

	expected := []interface{}{
		&Record{Map: map[string]string{"one": "un", "two": "deux"}},
		&Record{Map: map[string]string{}},
		&Record{Map: map[string]string{"three": "trois"}},
	}

	structBuilderTest(t, new(Record), new(Record), expected)
}

func TestStructBuilderTraces(t *testing.T) {
	type Tag struct {
		Name  string `parquet:"name=name, type=UTF8"`
		Value string `parquet:"name=value, type=UTF8"`
	}

	type Record struct {
		kafkaTopic     string   //not exporting it to parquet
		KafkaPartition int64    `parquet:"name=kafka_partition, type=INT_64"`
		KafkaOffset    int64    `parquet:"name=kafka_offset, type=INT_64"`
		TimestampMs    int64    `parquet:"name=timestamp_ms, type=INT_64"`
		TraceID        string   `parquet:"name=trace_id, type=UTF8"`
		SpanID         string   `parquet:"name=span_id, type=UTF8"`
		ParentSpanID   string   `parquet:"name=parent_span_id, type=UTF8"`
		NextTraceID    string   `parquet:"name=next_trace_id, type=UTF8"`
		ParentTraceID  []string `parquet:"name=parent_trace_id, type=LIST, valuetype=UTF8"`
		Baggage        []Tag    `parquet:"name=baggage, type=LIST"`
		Tags           []Tag    `parquet:"name=tags, type=LIST"`
		Attributes     []Tag    `parquet:"name=attributes, type=LIST"`
		Measures       []Tag    `parquet:"name=measures, type=LIST"`

		// Extracted from the exchange payload. Will be nil if they are not
		// present in the payload or if the payload cannot be unmarshalled.
		MessageID     string `parquet:"name=message_id, type=UTF8"`
		UserID        string `parquet:"name=user_id, type=UTF8"`
		EventType     string `parquet:"name=event_type, type=UTF8"`
		SourceID      string `parquet:"name=source_id, type=UTF8"`
		DestinationID string `parquet:"name=destination_id, type=UTF8"`
		WorkspaceID   string `parquet:"name=workspace_id, type=UTF8"`

		Name         string `parquet:"name=name, type=UTF8"`
		SpanTime     int64  `parquet:"name=span_time, type=INT_64"`     // ms
		SpanDuration int64  `parquet:"name=span_duration, type=INT_64"` // ms

		ExchangeTime     int64 `parquet:"name=exchange_time, type=INT_64"`     // ms
		ExchangeDuration int32 `parquet:"name=exchange_duration, type=INT_32"` // ms

		ExchangeRequestMethod  string            `parquet:"name=exchange_request_method, type=UTF8"`
		ExchangeRequestURL     string            `parquet:"name=exchange_request_url, type=UTF8"`
		ExchangeRequestHeaders map[string]string `parquet:"name=exchange_request_headers, type=MAP, keytype=UTF8, valuetype=UTF8"`
		ExchangeRequestBody    string            `parquet:"name=exchange_request_body, type=UTF8"`

		ExchangeResponseStatusCode uint16            `parquet:"name=exchange_response_status_code, type=UINT_16"`
		ExchangeResponseStatusText string            `parquet:"name=exchange_response_status_text, type=UTF8"`
		ExchangeResponseHeaders    map[string]string `parquet:"name=exchange_response_headers, type=UTF8, keytype=UTF8, valuetype=UTF8"`
		ExchangeResponseBody       string            `parquet:"name=exchange_response_body, type=UTF8"`

		ExchangeErrorType    string `parquet:"name=exchange_error_type, type=UTF8"`
		ExchangeErrorMessage string `parquet:"name=exchange_error_message, type=UTF8"`
	}
	expected := []interface{}{
		&Record{
			KafkaPartition: 1,
			KafkaOffset:    2,
			TimestampMs:    3,
			TraceID:        "TRACE_ID",
			SpanID:         "SPAN_ID",
			ParentSpanID:   "PARENT_SPAN_ID",
			NextTraceID:    "NEXT_TRACE_ID",
			ParentTraceID:  nil,
			Baggage: []Tag{
				{
					Name:  "BAGGAGE_1",
					Value: "BAGGAGE_UN",
				},
				{
					Name:  "BAGGAGE_2",
					Value: "BAGGAGE_DEUX",
				},
			},
			Tags: []Tag{
				{
					Name:  "TAGS_1",
					Value: "TAGS_UN",
				},
			},
			Attributes:            nil,
			Measures:              nil,
			MessageID:             "MESSAGE_ID",
			UserID:                "USER_ID",
			EventType:             "EVENT_TYPE",
			SourceID:              "SOURCE_ID",
			DestinationID:         "DESTINATION_ID",
			WorkspaceID:           "WORKSPACE_ID",
			Name:                  "NAME",
			SpanTime:              4,
			SpanDuration:          5,
			ExchangeTime:          6,
			ExchangeDuration:      7,
			ExchangeRequestMethod: "METHOD",
			ExchangeRequestURL:    "URL",
			ExchangeRequestHeaders: map[string]string{
				"HEADER_1": "UN",
			},
			ExchangeRequestBody:        "REQUEST",
			ExchangeResponseStatusCode: 8,
			ExchangeResponseStatusText: "STATUS",
			ExchangeResponseHeaders:    map[string]string{},
			ExchangeResponseBody:       "RESPONSE",
			ExchangeErrorType:          "",
			ExchangeErrorMessage:       "",
		},
	}

	structBuilderTest(t, new(Record), new(Record), expected)
}

func structBuilderTest(t *testing.T, recordPgo, record interface{}, expected []interface{}) {
	// We have to pass two different structs because the annotations are
	// different between this library and parquet-go. This should all go
	// away when we have our own writer.
	test.WithTestDir(t, func(dir string) {
		p := path.Join(dir, "test.parquet")
		dst, err := localref.NewLocalFileWriter(p)
		require.NoError(t, err)
		writer, err := writerref.NewParquetWriter(dst, recordPgo, 1)
		require.NoError(t, err)
		for _, record := range expected {
			require.NoError(t, writer.Write(record))
		}
		require.NoError(t, writer.WriteStop())
		require.NoError(t, dst.Close())

		f, err := os.Open(p)
		require.NoError(t, err)
		defer func() { assert.NoError(t, f.Close()) }()

		pf, err := parquet.OpenFile(f)
		require.NoError(t, err)

		// TODO: this is a lot of ceremony
		planner := parquet.StructPlannerOf(record)
		builder := planner.Builder()
		plan := planner.Plan()
		rowReader := parquet.NewRowReader(plan, pf)

		var actual []interface{}

		for {
			target := reflect.New(reflect.TypeOf(record).Elem()).Interface()
			err := rowReader.Read(builder.To(target))
			if err == parquet.EOF {
				break
			}
			require.NoError(t, err)
			actual = append(actual, target)
		}

		require.Equal(t, expected, actual)
	})
}
