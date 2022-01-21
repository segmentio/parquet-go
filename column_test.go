package parquet_test

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/format"
)

func TestColumnPageIndex(t *testing.T) {
	for _, config := range [...]struct {
		name string
		test func(*testing.T, rows) bool
	}{
		{
			name: "buffer",
			test: testColumnPageIndexWithBuffer,
		},
		{
			name: "file",
			test: testColumnPageIndexWithFile,
		},
	} {
		t.Run(config.name, func(t *testing.T) {
			for _, test := range [...]struct {
				scenario string
				function func(*testing.T) interface{}
			}{
				{
					scenario: "boolean",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value bool }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "int32",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value int32 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "int64",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value int64 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "int96",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value deprecated.Int96 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "uint32",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value uint32 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "uint64",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value uint64 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "float32",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value float32 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "float64",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value float64 }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "string",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value string }) bool { return config.test(t, makeRows(rows)) }
					},
				},

				{
					scenario: "uuid",
					function: func(t *testing.T) interface{} {
						return func(rows []struct{ Value uuid.UUID }) bool { return config.test(t, makeRows(rows)) }
					},
				},
			} {
				t.Run(test.scenario, func(t *testing.T) {
					if err := quick.Check(test.function(t), nil); err != nil {
						t.Error(err)
					}
				})
			}
		})
	}
}

func testColumnPageIndexWithBuffer(t *testing.T, rows rows) bool {
	if len(rows) > 0 {
		b := parquet.NewBuffer()
		for _, row := range rows {
			b.Write(row)
		}
		if err := checkRowGroupColumnIndex(b); err != nil {
			t.Error(err)
			return false
		}
		if err := checkRowGroupOffsetIndex(b); err != nil {
			t.Error(err)
			return false
		}
	}
	return true
}

func checkRowGroupColumnIndex(rowGroup parquet.RowGroup) error {
	for i, n := 0, rowGroup.NumColumns(); i < n; i++ {
		if err := checkColumnChunkColumnIndex(rowGroup.Column(i)); err != nil {
			return fmt.Errorf("column chunk @i=%d: %w", i, err)
		}
	}
	return nil
}

func checkColumnChunkColumnIndex(columnChunk parquet.ColumnChunk) error {
	columnType := columnChunk.Type()
	columnIndex := columnChunk.ColumnIndex()
	numPages := columnIndex.NumPages()
	pagesRead := 0
	err := forEachPage(columnChunk.Pages(), func(page parquet.Page) error {
		min, max := page.Bounds()
		pageMin := min.Bytes()
		pageMax := max.Bytes()
		indexMin := columnIndex.MinValue(pagesRead)
		indexMax := columnIndex.MaxValue(pagesRead)

		if !bytes.Equal(pageMin, indexMin) {
			return fmt.Errorf("max page value mismatch: index=%x page=%x", indexMin, pageMin)
		}
		if !bytes.Equal(pageMax, indexMax) {
			return fmt.Errorf("max page value mismatch: index=%x page=%x", indexMax, pageMax)
		}

		numNulls := int64(0)
		numValues := int64(0)
		err := forEachValue(page.Values(), func(value parquet.Value) error {
			if value.IsNull() {
				numNulls++
			}
			numValues++
			return nil
		})
		if err != nil {
			return err
		}

		nullCount := columnIndex.NullCount(pagesRead)
		if numNulls != nullCount {
			return fmt.Errorf("number of null values mimatch: index=%d page=%d", nullCount, numNulls)
		}

		nullPage := columnIndex.NullPage(pagesRead)
		if numNulls > 0 && numNulls == numValues && !nullPage {
			return fmt.Errorf("page only contained null values but the index did not categorize it as a null page: nulls=%d", numNulls)
		}

		switch {
		case columnIndex.IsAscending():
			if columnType.Compare(min, max) > 0 {
				return fmt.Errorf("column index is declared to be in ascending order but %v > %v", min, max)
			}
		case columnIndex.IsDescending():
			if columnType.Compare(min, max) < 0 {
				return fmt.Errorf("column index is declared to be in desending order but %v < %v", min, max)
			}
		}

		pagesRead++
		return nil
	})
	if err != nil {
		return fmt.Errorf("page @i=%d: %w", pagesRead, err)
	}
	if pagesRead != numPages {
		return fmt.Errorf("number of pages found in column index differs from the number of pages read: index=%d read=%d", numPages, pagesRead)
	}
	return nil
}

func checkRowGroupOffsetIndex(rowGroup parquet.RowGroup) error {
	for i, n := 0, rowGroup.NumColumns(); i < n; i++ {
		if err := checkColumnChunkOffsetIndex(rowGroup.Column(i)); err != nil {
			return fmt.Errorf("column chunk @i=%d: %w", i, err)
		}
	}
	return nil
}

func checkColumnChunkOffsetIndex(columnChunk parquet.ColumnChunk) error {
	offsetIndex := columnChunk.OffsetIndex()
	numPages := offsetIndex.NumPages()
	pagesRead := 0
	rowIndex := int64(0)

	err := forEachPage(columnChunk.Pages(), func(page parquet.Page) error {
		if firstRowIndex := offsetIndex.FirstRowIndex(pagesRead); firstRowIndex != rowIndex {
			return fmt.Errorf("row number mismatch: index=%d page=%d", firstRowIndex, rowIndex)
		}
		rowIndex += int64(page.NumRows())
		pagesRead++
		return nil
	})
	if err != nil {
		return fmt.Errorf("page @i=%d: %w", pagesRead, err)
	}

	if pagesRead != numPages {
		return fmt.Errorf("number of pages found in offset index differs from the number of pages read: index=%d read=%d", numPages, pagesRead)
	}

	return nil
}

func testColumnPageIndexWithFile(t *testing.T, rows rows) bool {
	if len(rows) > 0 {
		f, err := createParquetFile(rows)
		if err != nil {
			t.Error(err)
			return false
		}
		if err := checkFileColumnIndex(f); err != nil {
			t.Error(err)
			return false
		}
		if err := checkFileOffsetIndex(f); err != nil {
			t.Error(err)
			return false
		}
		for i, n := 0, f.NumRowGroups(); i < n; i++ {
			if err := checkRowGroupColumnIndex(f.RowGroup(i)); err != nil {
				t.Errorf("checking column index of row group @i=%d: %v", i, err)
				return false
			}
			if err := checkRowGroupOffsetIndex(f.RowGroup(i)); err != nil {
				t.Errorf("checking offset index of row group @i=%d: %v", i, err)
				return false
			}
		}
	}
	return true
}

func checkFileColumnIndex(f *parquet.File) error {
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

func checkFileOffsetIndex(f *parquet.File) error {
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
		index.PageLocations[i] = format.PageLocation{
			Offset:             offsetIndex.Offset(i),
			CompressedPageSize: int32(offsetIndex.CompressedPageSize(i)),
			FirstRowIndex:      offsetIndex.FirstRowIndex(i),
		}
	}

	return index
}
