package parquet_test

import (
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestTransformRowReader(t *testing.T) {
	rows := []parquet.Row{
		{parquet.Int64Value(0)},
		{parquet.Int64Value(1)},
		{parquet.Int64Value(2)},
		{parquet.Int64Value(3)},
		{parquet.Int64Value(4)},
	}

	want := []parquet.Row{
		{parquet.Int64Value(0)},
		{parquet.Int64Value(0)},
		{parquet.Int64Value(1)},
		{parquet.Int64Value(2)},
		{parquet.Int64Value(2)},
		{parquet.Int64Value(4)},
		{parquet.Int64Value(3)},
		{parquet.Int64Value(6)},
		{parquet.Int64Value(4)},
		{parquet.Int64Value(8)},
	}

	reader := parquet.TransformRowReader(&bufferedRows{rows: rows},
		func(dst []parquet.Row, src parquet.Row, rowIndex int64) (int, error) {
			dst[0] = append(dst[0], src[0])
			dst[1] = append(dst[1], parquet.Int64Value(2*src[0].Int64()))
			return 2, nil
		},
	)

	writer := &bufferedRows{}
	_, err := parquet.CopyRows(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	assertEqualRows(t, want, writer.rows)
}

func TestTransformRowWriter(t *testing.T) {
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
	writer := parquet.TransformRowWriter(buffer,
		func(dst []parquet.Row, src parquet.Row, rowIndex int64) (int, error) {
			if (src[0].Int64() % 2) == 0 {
				return 0, nil
			} else {
				dst[0] = append(dst[0], src[0])
				return 1, nil
			}
		},
	)

	reader := &bufferedRows{rows: rows}
	_, err := parquet.CopyRows(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	assertEqualRows(t, want, buffer.rows)
}
