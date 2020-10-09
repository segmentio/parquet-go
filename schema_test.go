package parquet_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/segmentio/parquet"
	"github.com/stretchr/testify/require"
)

func TestNodeStageDelete(t *testing.T) {
	file, err := os.Open("./examples/stage-small.parquet")
	require.NoError(t, err)
	defer file.Close()
	pf, err := parquet.OpenFile(file)
	require.NoError(t, err)
	root := pf.Metadata().Schema

	original := readableFlatTree(root)
	expected := `parquet_go_root
.kafka_partition
.kafka_offset
.timestamp_ms
.trace_id
.span_id
.parent_span_id
.next_trace_id
.parent_trace_ids
..list
...element
.baggage
..list
...element
....name
....value
.tags
..list
...element
....name
....value
.message_id
.user_id
.event_type
.source_id
.destination_id
.workspace_id
.name
.span_time
.span_duration
.exchange_time
.exchange_duration
.exchange_request_method
.exchange_request_url
.exchange_request_headers
..key_value
...key
...value
.exchange_request_body
.exchange_response_status_code
.exchange_response_status_text
.exchange_response_headers
..key_value
...key
...value
.exchange_response_body
.exchange_error_type
.exchange_error_message
`

	require.Equal(t, expected, original)

	// remove a top-level physical column
	root.At("name").Remove()

	// remove a group => remove all nodes under it.
	root.At("exchange_response_headers").Remove()

	// remove intermediate node => remove everything under it and up to the parent group
	root.At("baggage", "list").Remove()

	result := readableFlatTree(root)
	expected = `parquet_go_root
.kafka_partition
.kafka_offset
.timestamp_ms
.trace_id
.span_id
.parent_span_id
.next_trace_id
.parent_trace_ids
..list
...element
.tags
..list
...element
....name
....value
.message_id
.user_id
.event_type
.source_id
.destination_id
.workspace_id
.span_time
.span_duration
.exchange_time
.exchange_duration
.exchange_request_method
.exchange_request_url
.exchange_request_headers
..key_value
...key
...value
.exchange_request_body
.exchange_response_status_code
.exchange_response_status_text
.exchange_response_body
.exchange_error_type
.exchange_error_message
`

	require.Equal(t, expected, result)
}

func readableFlatTree(root *parquet.Schema) string {
	var b strings.Builder
	err := walk(root, func(n *parquet.Schema) error {
		b.WriteString(fmt.Sprintf("%s%s", strings.Repeat(".", len(n.Path)), n.Name))
		b.WriteRune('\n')
		return nil
	})
	if err != nil {
		panic("should not happen")
	}
	return b.String()
}

func walk(n *parquet.Schema, walkFn func(n *parquet.Schema) error) error {
	err := walkFn(n)
	if err != nil {
		return err
	}
	for _, c := range n.Children {
		err := walk(c, walkFn)
		if err != nil {
			return err
		}
	}

	return nil
}
