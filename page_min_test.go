package parquet

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/segmentio/parquet-go/internal/quick"
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

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
		return min == minInt32(values)
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
		return min == minInt64(values)
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
		return min == minUint32(values)
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
		return min == minUint64(values)
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
		return min == minFloat32(values)
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
		return min == minFloat64(values)
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
		ret := minFixedLenByteArray(1, values)
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
		ret := minFixedLenByteArray(16, unsafecast.Uint128ToBytes(values))
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
			minBool(values)
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
			minInt32(values)
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
			minInt64(values)
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
			minUint32(values)
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
			minUint64(values)
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
			minFloat32(values)
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
			minFloat64(values)
		}
	})
}

func BenchmarkMinBE128(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([][16]byte, bufferSize)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			prng.Read(values[i][:])
		}
		for i := 0; i < b.N; i++ {
			minBE128(values)
		}
	})
}

func BenchmarkMinFixedLenByteArray(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]byte, bufferSize)
		prng := rand.New(rand.NewSource(1))
		prng.Read(values)
		for i := 0; i < b.N; i++ {
			minFixedLenByteArray(16, values)
		}
	})
}
