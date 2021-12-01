package parquet_test

import (
	"fmt"
	"testing"
	"testing/quick"

	"github.com/google/uuid"
	"github.com/segmentio/parquet"
)

func TestColumnPageIndex(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T) interface{}
	}{
		{
			scenario: "boolean",
			function: func(t *testing.T) interface{} {
				return func(rows []struct{ Value bool }) bool { return testColumnPageIndex(t, makeRows(rows)) }
			},
		},

		{
			scenario: "int32",
			function: func(t *testing.T) interface{} {
				return func(rows []struct{ Value int32 }) bool { return testColumnPageIndex(t, makeRows(rows)) }
			},
		},

		{
			scenario: "int64",
			function: func(t *testing.T) interface{} {
				return func(rows []struct{ Value int64 }) bool { return testColumnPageIndex(t, makeRows(rows)) }
			},
		},

		{
			scenario: "uint32",
			function: func(t *testing.T) interface{} {
				return func(rows []struct{ Value uint32 }) bool { return testColumnPageIndex(t, makeRows(rows)) }
			},
		},

		{
			scenario: "uint64",
			function: func(t *testing.T) interface{} {
				return func(rows []struct{ Value uint64 }) bool { return testColumnPageIndex(t, makeRows(rows)) }
			},
		},

		{
			scenario: "float32",
			function: func(t *testing.T) interface{} {
				return func(rows []struct{ Value float32 }) bool { return testColumnPageIndex(t, makeRows(rows)) }
			},
		},

		{
			scenario: "float64",
			function: func(t *testing.T) interface{} {
				return func(rows []struct{ Value float64 }) bool { return testColumnPageIndex(t, makeRows(rows)) }
			},
		},

		{
			scenario: "string",
			function: func(t *testing.T) interface{} {
				return func(rows []struct{ Value string }) bool { return testColumnPageIndex(t, makeRows(rows)) }
			},
		},

		{
			scenario: "uuid",
			function: func(t *testing.T) interface{} {
				return func(rows []struct{ Value uuid.UUID }) bool { return testColumnPageIndex(t, makeRows(rows)) }
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			if err := quick.Check(test.function(t), nil); err != nil {
				t.Error(err)
			}
		})
	}
}

func testColumnPageIndex(t *testing.T, rows rows) bool {
	if len(rows) > 0 {
		f, err := createParquetFile(v2, rows)
		if err != nil {
			t.Error(err)
			return false
		}
		if err := checkColumnIndex(t, f); err != nil {
			t.Error(err)
			return false
		}
		if err := checkOffsetIndex(t, f); err != nil {
			t.Error(err)
			return false
		}
	}
	return true
}

func checkColumnIndex(t *testing.T, f *parquet.File) error {
	return forEachColumnChunk(f.Root(), func(col *parquet.Column, chunk *parquet.ColumnChunks) error {
		columnIndex, err := chunk.ReadColumnIndex()
		if err != nil {
			return err
		}
		if n := columnIndex.NumPages(); n <= 0 {
			return fmt.Errorf("invalid number of pages found in the column index: %d", n)
		}
		return nil
	})
}

func checkOffsetIndex(t *testing.T, f *parquet.File) error {
	return forEachColumnChunk(f.Root(), func(col *parquet.Column, chunk *parquet.ColumnChunks) error {
		offsetIndex, err := chunk.ReadOffsetIndex()
		if err != nil {
			return err
		}
		if n := offsetIndex.NumPages(); n <= 0 {
			return fmt.Errorf("invalid number of pages found in the offset index: %d", n)
		}
		return nil
	})
}
