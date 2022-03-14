package parquet_test

import (
	"sort"
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestSortFunc(t *testing.T) {
	sortFunc := parquet.SortFuncOf(parquet.String().Type(),
		parquet.SortMaxDefinitionLevel(1),
		parquet.SortDescending(true),
		parquet.SortNullsFirst(true),
	)

	values := [][]parquet.Value{
		{parquet.ValueOf("A")},
		{parquet.ValueOf(nil)},
		{parquet.ValueOf(nil)},
		{parquet.ValueOf("C")},
		{parquet.ValueOf("B")},
		{parquet.ValueOf(nil)},
	}

	expect := [][]parquet.Value{
		{parquet.ValueOf(nil)},
		{parquet.ValueOf(nil)},
		{parquet.ValueOf(nil)},
		{parquet.ValueOf("C")},
		{parquet.ValueOf("B")},
		{parquet.ValueOf("A")},
	}

	sort.Slice(values, func(i, j int) bool {
		return sortFunc(values[i], values[j]) < 0
	})

	for i := range values {
		if !parquet.Equal(values[i][0], expect[i][0]) {
			t.Errorf("value at index %d mismatch: got=%+v want=%+v\n%+v\n%+v", i, expect[i], values[i], expect, values)
			break
		}
	}
}
