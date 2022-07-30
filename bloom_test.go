package parquet

import (
	"math/rand"
	"testing"

	"github.com/segmentio/parquet-go/bloom"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/encoding/plain"
	"github.com/segmentio/parquet-go/internal/quick"
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

func TestSplitBlockFilter(t *testing.T) {
	newFilter := func(numValues int) bloom.SplitBlockFilter {
		return make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(int64(numValues), 11))
	}

	enc := SplitBlockFilter("$").Encoding()

	check := func(filter bloom.SplitBlockFilter, value Value) bool {
		return filter.Check(value.hash(&bloom.XXH64{}))
	}

	tests := []struct {
		scenario string
		function interface{}
	}{
		{
			scenario: "BOOLEAN",
			function: func(values []bool) bool {
				filter := newFilter(len(values))
				bits := unsafecast.BoolToBytes(values)
				enc.EncodeBoolean(filter.Bytes(), encoding.BooleanValues(bits))
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT32",
			function: func(values []int32) bool {
				filter := newFilter(len(values))
				enc.EncodeInt32(filter.Bytes(), encoding.Int32Values(values))
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT64",
			function: func(values []int64) bool {
				filter := newFilter(len(values))
				enc.EncodeInt64(filter.Bytes(), encoding.Int64Values(values))
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT96",
			function: func(values []deprecated.Int96) bool {
				filter := newFilter(len(values))
				enc.EncodeInt96(filter.Bytes(), encoding.Int96Values(values))
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "FLOAT",
			function: func(values []float32) bool {
				filter := newFilter(len(values))
				enc.EncodeFloat(filter.Bytes(), encoding.FloatValues(values))
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "DOUBLE",
			function: func(values []float64) bool {
				filter := newFilter(len(values))
				enc.EncodeDouble(filter.Bytes(), encoding.DoubleValues(values))
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "BYTE_ARRAY",
			function: func(values [][]byte) bool {
				byteArrays := make([]byte, 0)
				for _, value := range values {
					byteArrays = plain.AppendByteArray(byteArrays, value)
				}
				filter := newFilter(len(values))
				enc.EncodeByteArray(filter.Bytes(), encoding.ByteArrayValues(byteArrays, nil))
				for _, v := range values {
					if !check(filter, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "FIXED_LEN_BYTE_ARRAY",
			function: func(values []byte) bool {
				filter := newFilter(len(values))
				enc.EncodeFixedLenByteArray(filter.Bytes(), encoding.FixedLenByteArrayValues(values, 1))
				for _, v := range values {
					if !check(filter, ValueOf([1]byte{v})) {
						return false
					}
				}
				return true
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			if err := quick.Check(test.function); err != nil {
				t.Error(err)
			}
		})
	}
}

func BenchmarkSplitBlockFilter(b *testing.B) {
	const N = 1000
	f := make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(N, 10)).Bytes()
	e := SplitBlockFilter("$").Encoding()

	v := make([]int64, N)
	r := rand.NewSource(10)
	for i := range v {
		v[i] = r.Int63()
	}

	v64 := encoding.Int64Values(v)
	for i := 0; i < b.N; i++ {
		e.EncodeInt64(f, v64)
	}

	b.SetBytes(8 * N)
}
