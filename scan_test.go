package parquet_test

import (
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestScanRowReader(t *testing.T) {
	rows := []parquet.Row{
		{parquet.Int64Value(0)},
		{parquet.Int64Value(1)},
		{parquet.Int64Value(2)},
		{parquet.Int64Value(3)},
		{parquet.Int64Value(4)},
	}

	want := []parquet.Row{
		{parquet.Int64Value(0)},
		{parquet.Int64Value(1)},
		{parquet.Int64Value(2)},
	}

	reader := parquet.ScanRowReader(&bufferedRows{rows: rows},
		func(row parquet.Row, _ int64) bool {
			return row[0].Int64() < 3
		},
	)

	writer := &bufferedRows{}
	_, err := parquet.CopyRows(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	assertEqualRows(t, want, writer.rows)
}
