package bits_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/segmentio/parquet-go/internal/bits"
	"github.com/segmentio/parquet-go/internal/quick"
)

func TestMinBool(t *testing.T) {
	err := quick.Check(func(values []bool) bool {
		min := len(values) > 0
		for _, v := range values {
			if !v {
				min = false
				break
			}
		}
		return min == bits.MinBool(values)
	})
	if err != nil {
		t.Error(err)
	}

	values := make([]bool, 200)
	if bits.MinBool(values) {
		t.Error("min value must be false when all input values are false")
	}
	for i := range values {
		values[i] = true
	}
	if !bits.MinBool(values) {
		t.Error("min value must be true when all input values are true")
	}
}

func TestMinInt32(t *testing.T) {
	err := quick.Check(func(values []int32) bool {
		min := int32(0)
		if len(values) > 0 {
			min = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
			}
		}
		return min == bits.MinInt32(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinInt64(t *testing.T) {
	err := quick.Check(func(values []int64) bool {
		min := int64(0)
		if len(values) > 0 {
			min = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
			}
		}
		return min == bits.MinInt64(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinUint32(t *testing.T) {
	err := quick.Check(func(values []uint32) bool {
		min := uint32(0)
		if len(values) > 0 {
			min = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
			}
		}
		return min == bits.MinUint32(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinUint64(t *testing.T) {
	err := quick.Check(func(values []uint64) bool {
		min := uint64(0)
		if len(values) > 0 {
			min = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
			}
		}
		return min == bits.MinUint64(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinFloat32(t *testing.T) {
	err := quick.Check(func(values []float32) bool {
		min := float32(0)
		if len(values) > 0 {
			min = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
			}
		}
		return min == bits.MinFloat32(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinFloat64(t *testing.T) {
	err := quick.Check(func(values []float64) bool {
		min := float64(0)
		if len(values) > 0 {
			min = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
			}
		}
		return min == bits.MinFloat64(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinFixedLenByteArray1(t *testing.T) {
	err := quick.Check(func(values []byte) bool {
		min := [1]byte{}
		if len(values) > 0 {
			min[0] = values[0]
			for _, v := range values[1:] {
				if v < min[0] {
					min[0] = v
				}
			}
		}
		ret := bits.MinFixedLenByteArray(1, values)
		return (len(values) == 0 && ret == nil) || bytes.Equal(min[:], ret)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinFixedLenByteArray16(t *testing.T) {
	err := quick.Check(func(values [][16]byte) bool {
		min := [16]byte{}
		if len(values) > 0 {
			min = values[0]
			for _, v := range values[1:] {
				if bytes.Compare(v[:], min[:]) < 0 {
					min = v
				}
			}
		}
		ret := bits.MinFixedLenByteArray(16, bits.Uint128ToBytes(values))
		return (len(values) == 0 && ret == nil) || bytes.Equal(min[:], ret)
	})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkMinBool(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]bool, bufferSize/1)
		for i := range values {
			values[i] = true
		}
		for i := 0; i < b.N; i++ {
			bits.MinBool(values)
		}
	})
}

func BenchmarkMinInt32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Int31()
		}
		for i := 0; i < b.N; i++ {
			bits.MinInt32(values)
		}
	})
}

func BenchmarkMinInt64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Int63()
		}
		for i := 0; i < b.N; i++ {
			bits.MinInt64(values)
		}
	})
}

func BenchmarkMinUint32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Uint32()
		}
		for i := 0; i < b.N; i++ {
			bits.MinUint32(values)
		}
	})
}

func BenchmarkMinUint64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Uint64()
		}
		for i := 0; i < b.N; i++ {
			bits.MinUint64(values)
		}
	})
}

func BenchmarkMinFloat32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Float32()
		}
		for i := 0; i < b.N; i++ {
			bits.MinFloat32(values)
		}
	})
}

func BenchmarkMinFloat64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Float64()
		}
		for i := 0; i < b.N; i++ {
			bits.MinFloat64(values)
		}
	})
}

func BenchmarkMinFixedLenByteArray(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]byte, bufferSize)
		prng := rand.New(rand.NewSource(1))
		prng.Read(values)
		for i := 0; i < b.N; i++ {
			bits.MinFixedLenByteArray(16, values)
		}
	})
}
