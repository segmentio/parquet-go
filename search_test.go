package parquet_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/segmentio/parquet-go"
)

func assertCompare(t *testing.T, a, b parquet.Value, cmp func(parquet.Value, parquet.Value) int, want int) {
	if got := cmp(a, b); got != want {
		t.Errorf("compare(%v, %v): got=%d want=%d", a, b, got, want)
	}
}

func TestCompareNullsFirst(t *testing.T) {
	cmp := parquet.CompareNullsFirst(parquet.Int32Type.Compare)
	assertCompare(t, parquet.Value{}, parquet.Value{}, cmp, 0)
	assertCompare(t, parquet.Value{}, parquet.ValueOf(int32(0)), cmp, -1)
	assertCompare(t, parquet.ValueOf(int32(0)), parquet.Value{}, cmp, +1)
	assertCompare(t, parquet.ValueOf(int32(0)), parquet.ValueOf(int32(1)), cmp, -1)
}

func TestCompareNullsLast(t *testing.T) {
	cmp := parquet.CompareNullsLast(parquet.Int32Type.Compare)
	assertCompare(t, parquet.Value{}, parquet.Value{}, cmp, 0)
	assertCompare(t, parquet.Value{}, parquet.ValueOf(int32(0)), cmp, +1)
	assertCompare(t, parquet.ValueOf(int32(0)), parquet.Value{}, cmp, -1)
	assertCompare(t, parquet.ValueOf(int32(0)), parquet.ValueOf(int32(1)), cmp, -1)
}

func TestSearchBinary(t *testing.T) {
	testSearch(t, [][]int32{
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		{10, 10, 10, 10},
		{21, 22, 23, 24, 25},
		{30},
		{31},
		{32},
		{42, 43, 44, 45, 46, 47, 48, 49},
	})
}

func TestSearchLinear(t *testing.T) {
	testSearch(t, [][]int32{
		{10, 10, 10, 10},
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		{21, 22, 23, 24, 25},
		{19, 18, 17, 16, 15, 14, 13, 12, 11},
		{42, 43, 44, 45, 46, 47, 48, 49},
	})
}

func testSearch(t *testing.T, pages [][]int32) {
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

	for i, values := range pages {
		t.Run(fmt.Sprintf("page#%02d", i), func(t *testing.T) {
			for _, value := range values {
				v := parquet.ValueOf(value)
				j := parquet.Search(columnIndex, v, parquet.Int32Type)

				if i != j {
					t.Errorf("searching for value %v: got=%d want=%d", v, j, i)
				}
			}

			for _, test := range []int32{math.MinInt32, math.MaxInt32} {
				if page := parquet.Search(columnIndex, parquet.ValueOf(test), parquet.Int32Type); page != len(pages) {
					t.Errorf("search for non-existing value %v: got=%d want=%d", test, page, len(pages))
				}
			}
		})
	}
}
