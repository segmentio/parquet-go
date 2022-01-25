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

	newEncoder := func(numValues int) bloom.Encoder {
		return bloom.NewSplitBlockEncoder(
			make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(numValues, 11)),
		)
	}

	tests := []struct {
		scenario string
		function interface{}
	}{
		{
			scenario: "BOOLEAN",
			function: func(values []bool) bool {
				e := newEncoder(len(values))
				f := e.Filter()
				e.EncodeBoolean(values)
				for _, v := range values {
					b := make([]byte, 1)
					if v {
						b[0] = 1
					}
					if !f.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT8",
			function: func(values []int8) bool {
				e := newEncoder(len(values))
				f := e.Filter()
				e.EncodeInt8(values)
				for _, v := range values {
					b := make([]byte, 1)
					b[0] = byte(v)
					if !f.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT16",
			function: func(values []int16) bool {
				e := newEncoder(len(values))
				f := e.Filter()
				e.EncodeInt16(values)
				for _, v := range values {
					b := make([]byte, 2)
					binary.LittleEndian.PutUint16(b, uint16(v))
					if !f.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT32",
			function: func(values []int32) bool {
				e := newEncoder(len(values))
				f := e.Filter()
				e.EncodeInt32(values)
				for _, v := range values {
					b := make([]byte, 4)
					binary.LittleEndian.PutUint32(b, uint32(v))
					if !f.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT64",
			function: func(values []int64) bool {
				e := newEncoder(len(values))
				f := e.Filter()
				e.EncodeInt64(values)
				for _, v := range values {
					b := make([]byte, 8)
					binary.LittleEndian.PutUint64(b, uint64(v))
					if !f.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "INT96",
			function: func(values []deprecated.Int96) bool {
				e := newEncoder(len(values))
				f := e.Filter()
				e.EncodeInt96(values)
				for _, v := range values {
					b := make([]byte, 12)
					binary.LittleEndian.PutUint32(b[0:4], v[0])
					binary.LittleEndian.PutUint32(b[4:8], v[1])
					binary.LittleEndian.PutUint32(b[8:12], v[2])
					if !f.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "FLOAT",
			function: func(values []float32) bool {
				e := newEncoder(len(values))
				f := e.Filter()
				e.EncodeFloat(values)
				for _, v := range values {
					b := make([]byte, 4)
					binary.LittleEndian.PutUint32(b, math.Float32bits(v))
					if !f.Check(hash(b)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "DOUBLE",
			function: func(values []float64) bool {
				e := newEncoder(len(values))
				f := e.Filter()
				e.EncodeDouble(values)
				for _, v := range values {
					b := make([]byte, 8)
					binary.LittleEndian.PutUint64(b, math.Float64bits(v))
					if !f.Check(hash(b)) {
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
				e := newEncoder(len(values))
				f := e.Filter()
				e.EncodeByteArray(a)
				for _, v := range values {
					if !f.Check(hash(v)) {
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
				e := newEncoder(len(values))
				f := e.Filter()
				e.EncodeByteArray(a)
				for _, v := range values {
					if !f.Check(hash(v)) {
						return false
					}
				}
				return true
			},
		},

		{
			scenario: "VALUE",
			function: func(values [][]byte) bool {
				e := newEncoder(len(values))
				f := e.Filter()
				for _, v := range values {
					if e.Encode(v) != nil {
						return false
					}
				}
				for _, v := range values {
					if !f.Check(hash(v)) {
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
		e := newEncoder(1)
		e.EncodeBoolean([]bool{false, true})

		allZeros := true
		for _, b := range e.Bytes() {
			if b != 0 {
				allZeros = false
				break
			}
		}
		if allZeros {
			t.Fatal("bloom filter bytes were all zero after encoding values")
		}

		e.Reset(nil)
		for i, b := range e.Bytes() {
			if b != 0 {
				t.Fatalf("bloom filter byte at index %d was not zero after resetting the encoder: %02X", i, b)
			}
		}
	})
}

func BenchmarkEncoder(b *testing.B) {
	const N = 1000
	e := bloom.NewSplitBlockEncoder(
		make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(N, 10)),
	)

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
