package parquet

import (
	"encoding/binary"
	"math"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/segmentio/parquet-go/bloom"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
)

func TestBloomFilterEncoder(t *testing.T) {
	newFilter := func(numValues int) *bloomFilterEncoder {
		return &bloomFilterEncoder{
			filter: make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(int64(numValues), 11)),
		}
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
					b := make([]byte, 1)
					if v {
						b[0] = 1
					}
					if !f.Check(b) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT8",
			function: func(values []int8) bool {
				f := newFilter(len(values))
				f.EncodeInt8(values)
				for _, v := range values {
					b := make([]byte, 1)
					b[0] = byte(v)
					if !f.Check(b) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT16",
			function: func(values []int16) bool {
				f := newFilter(len(values))
				f.EncodeInt16(values)
				for _, v := range values {
					b := make([]byte, 2)
					binary.LittleEndian.PutUint16(b, uint16(v))
					if !f.Check(b) {
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
					b := make([]byte, 4)
					binary.LittleEndian.PutUint32(b, uint32(v))
					if !f.Check(b) {
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
					b := make([]byte, 8)
					binary.LittleEndian.PutUint64(b, uint64(v))
					if !f.Check(b) {
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
					b := make([]byte, 12)
					binary.LittleEndian.PutUint32(b[0:4], v[0])
					binary.LittleEndian.PutUint32(b[4:8], v[1])
					binary.LittleEndian.PutUint32(b[8:12], v[2])
					if !f.Check(b) {
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
					b := make([]byte, 4)
					binary.LittleEndian.PutUint32(b, math.Float32bits(v))
					if !f.Check(b) {
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
					b := make([]byte, 8)
					binary.LittleEndian.PutUint64(b, math.Float64bits(v))
					if !f.Check(b) {
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
					if !f.Check(v) {
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
				f := newFilter(len(values))
				f.EncodeByteArray(a)
				for _, v := range values {
					if !f.Check(v) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "VALUE",
			function: func(values [][]byte) bool {
				f := newFilter(len(values))
				for _, v := range values {
					if f.Encode(v) != nil {
						return false
					}
				}
				for _, v := range values {
					if !f.Check(v) {
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
	f := &bloomFilterEncoder{
		filter: make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(N, 10)),
	}

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
