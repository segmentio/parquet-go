package parquet_test

import (
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestFilterRowReader(t *testing.T) {
	rows := []parquet.Row{
		{parquet.Int64Value(0)},
		{parquet.Int64Value(1)},
		{parquet.Int64Value(2)},
		{parquet.Int64Value(3)},
		{parquet.Int64Value(4)},
	}

	want := []parquet.Row{
		{parquet.Int64Value(0)},
		{parquet.Int64Value(2)},
		{parquet.Int64Value(4)},
	}

	reader := parquet.FilterRowReader(&bufferedRows{rows: rows},
		func(row parquet.Row) bool {
			return row[0].Int64()%2 == 0
		},
	)

	writer := &bufferedRows{}
	_, err := parquet.CopyRows(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	assertEqualRows(t, want, writer.rows)
}

func TestFilterRowWriter(t *testing.T) {
	rows := []parquet.Row{
		{parquet.Int64Value(0)},
		{parquet.Int64Value(1)},
		{parquet.Int64Value(2)},
		{parquet.Int64Value(3)},
		{parquet.Int64Value(4)},
	}

	want := []parquet.Row{
		{parquet.Int64Value(1)},
		{parquet.Int64Value(3)},
	}

	buffer := &bufferedRows{}
	writer := parquet.FilterRowWriter(buffer,
		func(row parquet.Row) bool {
			return row[0].Int64()%2 == 1
		},
	)

	reader := &bufferedRows{rows: rows}
	_, err := parquet.CopyRows(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	assertEqualRows(t, want, buffer.rows)
}
