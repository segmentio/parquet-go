package parquet_test

import (
	"strings"
	"testing"

	"github.com/segmentio/parquet"
)

type TestPathStruct struct {
	FirstName string
	LastName  string
}

func TestPath(t *testing.T) {
	testPathStruct := TestPathStruct{
		FirstName: "Luke",
		LastName:  "Skywalker",
	}

	tests := []struct {
		schema parquet.Node
		row    parquet.Row
		values [][]parquet.Value
	}{
		{
			schema: parquet.Group{
				"first_name": parquet.UTF8(),
				"last_name":  parquet.UTF8(),
			},
			row: parquet.RowOf(map[string]string{
				"first_name": "Luke",
				"last_name":  "Skywalker",
			}),
			values: [][]parquet.Value{
				0: {parquet.ValueOf(parquet.ByteArray, "Luke")},
				1: {parquet.ValueOf(parquet.ByteArray, "Skywalker")},
			},
		},

		{
			schema: parquet.SchemaOf(testPathStruct),
			row:    parquet.RowOf(testPathStruct),
			values: [][]parquet.Value{
				0: {parquet.ValueOf(parquet.ByteArray, "Luke")},
				1: {parquet.ValueOf(parquet.ByteArray, "Skywalker")},
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			paths := parquet.PathsOf(test.schema)

			for i, path := range paths {
				numValues := path.NumValues(test.row)

				if len(test.values[i]) != numValues {
					t.Errorf("number of values at %q mismatches: want=%d got=%d", joinPath(path.Path()), len(test.values[i]), numValues)
					continue
				}

				for index, value := range test.values[i] {
					pathValue := path.ValueIndex(test.row, index)

					if !parquet.Equal(pathValue, value) {
						t.Errorf("value at index %d mismatches: want=%s got=%s", index, value, pathValue)
					}
				}
			}
		})
	}
}

func joinPath(path []string) string {
	return strings.Join(path, ".")
}
