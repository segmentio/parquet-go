package parquet_test

import (
	"math"
	"testing"
	"unsafe"

	"github.com/segmentio/parquet-go"
)

func TestSizeOfValue(t *testing.T) {
	t.Logf("sizeof(parquet.Value) = %d", unsafe.Sizeof(parquet.Value{}))
}

func BenchmarkValueAppend(b *testing.B) {
	const N = 1024
	row := make(parquet.Row, 0, N)
	val := parquet.ValueOf(42)

	for i := 0; i < b.N; i++ {
		row = row[:0]
		for j := 0; j < N; j++ {
			row = append(row, val)
		}
	}

	b.SetBytes(N * int64(unsafe.Sizeof(parquet.Value{})))
}

func TestValueClone(t *testing.T) {
	tests := []struct {
		scenario string
		values   []interface{}
	}{
		{
			scenario: "BOOLEAN",
			values:   []interface{}{false, true},
		},

		{
			scenario: "INT32",
			values:   []interface{}{int32(0), int32(1), int32(math.MinInt32), int32(math.MaxInt32)},
		},

		{
			scenario: "INT64",
			values:   []interface{}{int64(0), int64(1), int64(math.MinInt64), int64(math.MaxInt64)},
		},

		{
			scenario: "FLOAT",
			values:   []interface{}{float32(0), float32(1), float32(-1)},
		},

		{
			scenario: "DOUBLE",
			values:   []interface{}{float64(0), float64(1), float64(-1)},
		},

		{
			scenario: "BYTE_ARRAY",
			values:   []interface{}{"", "A", "ABC", "Hello World!"},
		},

		{
			scenario: "FIXED_LEN_BYTE_ARRAY",
			values:   []interface{}{[1]byte{42}, [16]byte{0: 1}},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			for _, value := range test.values {
				v := parquet.ValueOf(value)
				c := v.Clone()

				if !parquet.DeepEqual(v, c) {
					t.Errorf("cloned values are not equal: want=%#v got=%#v", v, c)
				}
				if v.RepetitionLevel() != c.RepetitionLevel() {
					t.Error("cloned values do not have the same repetition level")
				}
				if v.DefinitionLevel() != c.DefinitionLevel() {
					t.Error("cloned values do not have the same definition level")
				}
				if v.Column() != c.Column() {
					t.Error("cloned values do not have the same column index")
				}
			}
		})
	}
}
