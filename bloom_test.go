package parquet

import (
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/segmentio/parquet-go/bloom"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding/plain"
	"github.com/segmentio/parquet-go/internal/bits"
)

func TestSplitBlockFilter(t *testing.T) {
	newFilter := func(numValues int) bloom.SplitBlockFilter {
		return make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(int64(numValues), 11))
	}

	encoding := SplitBlockFilter("$").Encoding()

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
				encoding.EncodeBoolean(filter.Bytes(), values)
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
				encoding.EncodeInt32(filter.Bytes(), values)
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
				encoding.EncodeInt64(filter.Bytes(), values)
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
				encoding.EncodeInt96(filter.Bytes(), deprecated.Int96ToBytes(values))
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
				encoding.EncodeFloat(filter.Bytes(), bits.Float32ToBytes(values))
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
				encoding.EncodeDouble(filter.Bytes(), bits.Float64ToBytes(values))
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
				encoding.EncodeByteArray(filter.Bytes(), byteArrays)
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
				encoding.EncodeFixedLenByteArray(filter.Bytes(), values, 1)
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
			if err := quick.Check(test.function, nil); err != nil {
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

	for i := 0; i < b.N; i++ {
		e.EncodeInt64(f, v)
	}

	b.SetBytes(8 * N)
}
