package parquet_test

import (
	"sort"
	"testing"

	"github.com/google/uuid"
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

func TestRepeatedUUIDSortFunc(t *testing.T) {
	type testStruct struct {
		List []uuid.UUID `parquet:"list"`
	}

	s := parquet.SchemaOf(&testStruct{})

	a := s.Deconstruct(nil, &testStruct{
		List: []uuid.UUID{
			uuid.MustParse("00000000-0000-0000-0000-000000000001"),
			uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		},
	})

	b := s.Deconstruct(nil, &testStruct{
		List: []uuid.UUID{
			uuid.MustParse("00000000-0000-0000-0000-000000000001"),
			uuid.MustParse("00000000-0000-0000-0000-000000000002"),
			uuid.MustParse("00000000-0000-0000-0000-000000000003"),
		},
	})

	// a and b are equal up until the third element, then a ends so a < b.
	f := parquet.SortFuncOf(
		s.Fields()[0].Type(),
		parquet.SortDescending(false),
		parquet.SortNullsFirst(true),
		parquet.SortMaxDefinitionLevel(1),
		parquet.SortMaxRepetitionLevel(1),
	)
	cmp := f(a, b)
	if cmp >= 0 {
		t.Fatal("expected a < b, got compare value", cmp)
	}
}
