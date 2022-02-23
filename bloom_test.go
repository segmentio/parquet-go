package parquet

import (
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/segmentio/parquet-go/bloom"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
)

func TestBloomFilterEncoder(t *testing.T) {
	newFilter := func(numValues int) *bloomFilterEncoder {
		return newBloomFilterEncoder(
			make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(int64(numValues), 11)),
			bloom.XXH64{},
		)
	}

	check := func(e *bloomFilterEncoder, v Value) bool {
		return e.filter.Check(v.hash(e.hash))
	}

	tests := []struct {
		scenario string
		function interface{}
	}{
		{
			scenario: "BOOLEAN",
			function: func(values []bool) bool {
				f := newFilter(len(values))
				f.EncodeBoolean(values)
				for _, v := range values {
					if !check(f, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT32",
			function: func(values []int32) bool {
				f := newFilter(len(values))
				f.EncodeInt32(values)
				for _, v := range values {
					if !check(f, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT64",
			function: func(values []int64) bool {
				f := newFilter(len(values))
				f.EncodeInt64(values)
				for _, v := range values {
					if !check(f, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT96",
			function: func(values []deprecated.Int96) bool {
				f := newFilter(len(values))
				f.EncodeInt96(values)
				for _, v := range values {
					if !check(f, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "FLOAT",
			function: func(values []float32) bool {
				f := newFilter(len(values))
				f.EncodeFloat(values)
				for _, v := range values {
					if !check(f, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "DOUBLE",
			function: func(values []float64) bool {
				f := newFilter(len(values))
				f.EncodeDouble(values)
				for _, v := range values {
					if !check(f, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "BYTE_ARRAY",
			function: func(values [][]byte) bool {
				a := encoding.ByteArrayList{}
				for _, v := range values {
					a.Push(v)
				}
				f := newFilter(len(values))
				f.EncodeByteArray(a)
				for _, v := range values {
					if !check(f, ValueOf(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "FIXED_LEN_BYTE_ARRAY",
			function: func(values []byte) bool {
				f := newFilter(len(values))
				f.EncodeFixedLenByteArray(1, values)
				for _, v := range values {
					if !check(f, ValueOf([1]byte{v})) {
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
				t.Fatal(err)
			}
		})
	}

	t.Run("Reset", func(t *testing.T) {
		f := newFilter(1)
		f.EncodeBoolean([]bool{false, true})

		allZeros := true
		for _, b := range f.Bytes() {
			if b != 0 {
				allZeros = false
				break
			}
		}
		if allZeros {
			t.Fatal("bloom filter bytes were all zero after encoding values")
		}

		f.Reset(nil)
		for i, b := range f.Bytes() {
			if b != 0 {
				t.Fatalf("bloom filter byte at index %d was not zero after resetting the encoder: %02X", i, b)
			}
		}
	})
}

func BenchmarkBloomFilterEncoder(b *testing.B) {
	const N = 1000
	f := newBloomFilterEncoder(
		make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(N, 10)),
		bloom.XXH64{},
	)

	v := make([]int64, N)
	r := rand.NewSource(10)
	for i := range v {
		v[i] = r.Int63()
	}

	for i := 0; i < b.N; i++ {
		f.EncodeInt64(v)
	}

	b.SetBytes(8 * N)
}
