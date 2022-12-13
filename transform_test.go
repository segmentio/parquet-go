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
		{parquet.Int64Value(0), parquet.Int64Value(0).Level(0, 0, 1)},
		{parquet.Int64Value(1), parquet.Int64Value(2).Level(0, 0, 1)},
		{parquet.Int64Value(2), parquet.Int64Value(4).Level(0, 0, 1)},
		{parquet.Int64Value(3), parquet.Int64Value(6).Level(0, 0, 1)},
		{parquet.Int64Value(4), parquet.Int64Value(8).Level(0, 0, 1)},
	}

	reader := parquet.TransformRowReader(&bufferedRows{rows: rows},
		func(dst, src parquet.Row) (parquet.Row, error) {
			dst = append(dst, src[0])
			dst = append(dst, parquet.Int64Value(2*src[0].Int64()).Level(0, 0, 1))
			return dst, nil
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
		func(dst, src parquet.Row) (parquet.Row, error) {
			if (src[0].Int64() % 2) != 0 {
				dst = append(dst, src[0])
			}
			return dst, nil
		},
	)

	reader := &bufferedRows{rows: rows}
	_, err := parquet.CopyRows(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	assertEqualRows(t, want, buffer.rows)
}
