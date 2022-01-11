package parquet_test

import (
	"fmt"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/google/uuid"
	"github.com/segmentio/parquet"
	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/format"
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
			scenario: "int96",
			function: func(t *testing.T) interface{} {
				return func(rows []struct{ Value deprecated.Int96 }) bool { return testColumnPageIndex(t, makeRows(rows)) }
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
		f, err := createParquetFile(rows)
		if err != nil {
			t.Error(err)
			return false
		}
		if err := testColumnIndex(t, f); err != nil {
			t.Error(err)
			return false
		}
		if err := testOffsetIndex(t, f); err != nil {
			t.Error(err)
			return false
		}
	}
	return true
}

func testColumnIndex(t *testing.T, f *parquet.File) error {
	columnIndexes := f.ColumnIndexes()
	i := 0
	return forEachColumnChunk(f, func(col *parquet.Column, chunk parquet.ColumnChunk) error {
		columnIndex := chunk.ColumnIndex()
		if n := columnIndex.NumPages(); n <= 0 {
			return fmt.Errorf("invalid number of pages found in the column index: %d", n)
		}
		if i >= len(columnIndexes) {
			return fmt.Errorf("more column indexes were read when iterating over column chunks than when reading from the file (i=%d,n=%d)", i, len(columnIndexes))
		}
		if !reflect.DeepEqual(&columnIndexes[i], newColumnIndex(columnIndex)) {
			return fmt.Errorf("column index at index %d mismatch:\nfile  = %#v\nchunk = %#v", i, &columnIndexes[i], columnIndex)
		}
		i++
		return nil
	})
}

func testOffsetIndex(t *testing.T, f *parquet.File) error {
	offsetIndexes := f.OffsetIndexes()
	i := 0
	return forEachColumnChunk(f, func(col *parquet.Column, chunk parquet.ColumnChunk) error {
		offsetIndex := chunk.OffsetIndex()
		if n := offsetIndex.NumPages(); n <= 0 {
			return fmt.Errorf("invalid number of pages found in the offset index: %d", n)
		}
		if i >= len(offsetIndexes) {
			return fmt.Errorf("more offset indexes were read when iterating over column chunks than when reading from the file (i=%d,n=%d)", i, len(offsetIndexes))
		}
		if !reflect.DeepEqual(&offsetIndexes[i], newOffsetIndex(offsetIndex)) {
			return fmt.Errorf("offset index at index %d mismatch:\nfile  = %#v\nchunk = %#v", i, &offsetIndexes[i], offsetIndex)
		}
		i++
		return nil
	})
}

func newColumnIndex(columnIndex parquet.ColumnIndex) *format.ColumnIndex {
	numPages := columnIndex.NumPages()
	index := &format.ColumnIndex{
		NullPages:  make([]bool, numPages),
		MinValues:  make([][]byte, numPages),
		MaxValues:  make([][]byte, numPages),
		NullCounts: make([]int64, numPages),
	}

	for i := 0; i < numPages; i++ {
		index.NullPages[i] = columnIndex.NullPage(i)
		index.MinValues[i] = columnIndex.MinValue(i)
		index.MaxValues[i] = columnIndex.MaxValue(i)
		index.NullCounts[i] = columnIndex.NullCount(i)
	}

	switch {
	case columnIndex.IsAscending():
		index.BoundaryOrder = format.Ascending
	case columnIndex.IsDescending():
		index.BoundaryOrder = format.Descending
	}

	return index
}

func newOffsetIndex(offsetIndex parquet.OffsetIndex) *format.OffsetIndex {
	index := &format.OffsetIndex{
		PageLocations: make([]format.PageLocation, offsetIndex.NumPages()),
	}

	for i := range index.PageLocations {
		offset, compressedPageSize, firstRowIndex := offsetIndex.PageLocation(i)
		index.PageLocations[i] = format.PageLocation{
			Offset:             offset,
			CompressedPageSize: int32(compressedPageSize),
			FirstRowIndex:      firstRowIndex,
		}
	}

	return index
}
