package bloom_test

import (
	"encoding/binary"
	"math"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/cespare/xxhash/v2"
	"github.com/segmentio/parquet-go/bloom"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
)

func TestEncoder(t *testing.T) {
	hash := xxhash.Sum64

	makeEncoder := func(numValues int) bloom.Encoder {
		return bloom.Encoder{
			Filter: make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(numValues, 11)),
		}
	}

	tests := []struct {
		scenario string
		function interface{}
	}{
		{
			scenario: "BOOLEAN",
			function: func(values []bool) bool {
				e := makeEncoder(len(values))
				e.EncodeBoolean(values)
				for _, v := range values {
					b := make([]byte, 1)
					if v {
						b[0] = 1
					}
					if !e.Filter.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT8",
			function: func(values []int8) bool {
				e := makeEncoder(len(values))
				e.EncodeInt8(values)
				for _, v := range values {
					b := make([]byte, 1)
					b[0] = byte(v)
					if !e.Filter.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT16",
			function: func(values []int16) bool {
				e := makeEncoder(len(values))
				e.EncodeInt16(values)
				for _, v := range values {
					b := make([]byte, 2)
					binary.LittleEndian.PutUint16(b, uint16(v))
					if !e.Filter.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT32",
			function: func(values []int32) bool {
				e := makeEncoder(len(values))
				e.EncodeInt32(values)
				for _, v := range values {
					b := make([]byte, 4)
					binary.LittleEndian.PutUint32(b, uint32(v))
					if !e.Filter.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT64",
			function: func(values []int64) bool {
				e := makeEncoder(len(values))
				e.EncodeInt64(values)
				for _, v := range values {
					b := make([]byte, 8)
					binary.LittleEndian.PutUint64(b, uint64(v))
					if !e.Filter.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT96",
			function: func(values []deprecated.Int96) bool {
				e := makeEncoder(len(values))
				e.EncodeInt96(values)
				for _, v := range values {
					b := make([]byte, 12)
					binary.LittleEndian.PutUint32(b[0:4], v[0])
					binary.LittleEndian.PutUint32(b[4:8], v[1])
					binary.LittleEndian.PutUint32(b[8:12], v[2])
					if !e.Filter.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "FLOAT",
			function: func(values []float32) bool {
				e := makeEncoder(len(values))
				e.EncodeFloat(values)
				for _, v := range values {
					b := make([]byte, 4)
					binary.LittleEndian.PutUint32(b, math.Float32bits(v))
					if !e.Filter.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "DOUBLE",
			function: func(values []float64) bool {
				e := makeEncoder(len(values))
				e.EncodeDouble(values)
				for _, v := range values {
					b := make([]byte, 8)
					binary.LittleEndian.PutUint64(b, math.Float64bits(v))
					if !e.Filter.Check(hash(b)) {
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
				e := makeEncoder(len(values))
				e.EncodeByteArray(a)
				for _, v := range values {
					if !e.Filter.Check(hash(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "FIXED_LEN_BYTE_ARRAY",
			function: func(values [][]byte) bool {
				a := encoding.ByteArrayList{}
				for _, v := range values {
					a.Push(v)
				}
				e := makeEncoder(len(values))
				e.EncodeByteArray(a)
				for _, v := range values {
					if !e.Filter.Check(hash(v)) {
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
}

func BenchmarkEncoder(b *testing.B) {
	const N = 1000
	e := bloom.Encoder{
		Filter: make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(N, 10)),
	}

	v := make([]int64, N)
	r := rand.NewSource(10)
	for i := range v {
		v[i] = r.Int63()
	}

	for i := 0; i < b.N; i++ {
		e.EncodeInt64(v)
	}

	b.SetBytes(8 * N)
}
