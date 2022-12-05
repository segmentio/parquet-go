package parquet_test

import (
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestSearchBinary(t *testing.T) {
	testSearch(t, [][]int32{
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		{10, 10, 10, 10},
		{21, 22, 24, 25, 30},
		{30, 30},
		{30, 31},
		{32},
		{42, 43, 44, 45, 46, 47, 48, 49},
	}, [][]int{
		{10, 1},
		{0, 0},
		{9, 0},
		// non-existant, but would be in this page
		{23, 2},
		// ensure we find the first page
		{30, 2},
		{31, 4},
		// out of bounds
		{99, 7},
		// out of bounds
		{-1, 7},
	})
}

func TestSearchLinear(t *testing.T) {
	testSearch(t, [][]int32{
		{10, 10, 10, 10},
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		{21, 22, 23, 24, 25},
		{19, 18, 17, 16, 14, 13, 12, 11},
		{42, 43, 44, 45, 46, 47, 48, 49},
	}, [][]int{
		{10, 0},
		{0, 1},
		{9, 1},
		{48, 4},
		// non-existant, but could be in this page
		{15, 3},
		// out of bounds
		{99, 5},
		// out of bounds
		{-1, 5},
	})
}

func testSearch(t *testing.T, pages [][]int32, expectIndex [][]int) {
	indexer := parquet.Int32Type.NewColumnIndexer(0)

	for _, values := range pages {
		min := values[0]
		max := values[0]

		for _, v := range values[1:] {
			switch {
			case v < min:
				min = v
			case v > max:
				max = v
			}
		}

		indexer.IndexPage(int64(len(values)), 0,
			parquet.ValueOf(min),
			parquet.ValueOf(max),
		)
	}

	formatIndex := indexer.ColumnIndex()
	columnIndex := parquet.NewColumnIndex(parquet.Int32, &formatIndex)

	for _, values := range expectIndex {
		v := parquet.ValueOf(values[0])
		j := parquet.Search(columnIndex, v, parquet.Int32Type)

		if values[1] != j {
			t.Errorf("searching for value %v: got=%d want=%d", v, j, values[1])
		}
	}
}
