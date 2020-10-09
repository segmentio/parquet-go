package parquet_test

import (
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/segmentio/parquet"
	"github.com/segmentio/parquet/internal/benchmark"
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

	structBuilderTest(t, new(Record), new(Record), expected, expected)
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

	structBuilderTest(t, new(Record), new(Record), expected, expected)
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

	structBuilderTest(t, new(Record), new(Record), expected, expected)
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

	structBuilderTest(t, new(Record), new(Record), expected, expected)
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

	structBuilderTest(t, new(Record), new(Record), expected, expected)
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

	structBuilderTest(t, new(Record), new(Record), expected, expected)
}

func TestStructBuilderAnonymousStruct(t *testing.T) {
	type Record struct {
		Inner struct {
			Value int32 `parquet:"name=value, type=INT_32"`
		} `parquet:"name=inner"`
	}

	expected := []interface{}{
		&Record{Inner: struct {
			Value int32 `parquet:"name=value, type=INT_32"`
		}{Value: 42}},
	}

	structBuilderTest(t, new(Record), new(Record), expected, expected)
}

func TestStructBuilderAnonymousField(t *testing.T) {
	type Anon struct {
		Avalue int32 `parquet:"name=avalue, type=INT_32"`
	}
	type Record struct {
		Anon  `parquet:"name=anon"`
		Value int32 `parquet:"name=value, type=INT_32"`
	}

	expected := []interface{}{
		&Record{
			Anon:  Anon{Avalue: 1},
			Value: 2,
		},
	}

	structBuilderTest(t, new(Record), new(Record), expected, expected)
}

func TestStructBuilderIgnoreNonExported(t *testing.T) {
	type Record struct {
		ignored int32
		Value   int32 `parquet:"name=value, type=INT_32"`
	}

	records := []interface{}{
		&Record{
			ignored: 1,
			Value:   2,
		},
	}

	expected := []interface{}{
		&Record{
			ignored: 0,
			Value:   2,
		},
	}

	structBuilderTest(t, new(Record), new(Record), expected, records)
}

func TestStructBuilderTraces(t *testing.T) {
	expected := []interface{}{
		&benchmark.Trace{
			KafkaPartition: 1,
			KafkaOffset:    2,
			TimestampMs:    3,
			TraceID:        "TRACE_ID",
			SpanID:         "SPAN_ID",
			ParentSpanID:   "PARENT_SPAN_ID",
			NextTraceID:    "NEXT_TRACE_ID",
			ParentTraceIDs: nil,
			Baggage: []benchmark.Tag{
				{
					Name:  "BAGGAGE_1",
					Value: "BAGGAGE_UN",
				},
				{
					Name:  "BAGGAGE_2",
					Value: "BAGGAGE_DEUX",
				},
			},
			Tags: []benchmark.Tag{
				{
					Name:  "TAGS_1",
					Value: "TAGS_UN",
				},
			},
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

	structBuilderTest(t, new(benchmark.Trace), new(benchmark.Trace), expected, expected)
}

func structBuilderTest(t *testing.T, recordPgo, record interface{}, expected []interface{}, records []interface{}) {
	// We have to pass two different structs because the annotations are
	// different between this library and parquet-go. This should all go
	// away when we have our own writer.
	test.WithTestDir(t, func(dir string) {
		p := path.Join(dir, "test.parquet")
		dst, err := localref.NewLocalFileWriter(p)
		require.NoError(t, err)
		writer, err := writerref.NewParquetWriter(dst, recordPgo, 1)
		require.NoError(t, err)
		for _, record := range records {
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
