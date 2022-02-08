package bits_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/segmentio/parquet-go/internal/bits"
)

func TestMaxBool(t *testing.T) {
	err := quickCheck(func(values []bool) bool {
		max := false
		for _, v := range values {
			if v {
				max = true
				break
			}
		}
		return max == bits.MaxBool(values)
	})
	if err != nil {
		t.Error(err)
	}

	values := make([]bool, 200)
	if bits.MaxBool(values) {
		t.Error("max value must be false when all input values are false")
	}
	for i := range values {
		values[i] = true
	}
	if !bits.MaxBool(values) {
		t.Error("max value must be true when all input values are true")
	}
}

func TestMaxInt32(t *testing.T) {
	err := quickCheck(func(values []int32) bool {
		max := int32(0)
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if v > max {
					max = v
				}
			}
		}
		return max == bits.MaxInt32(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxInt64(t *testing.T) {
	err := quickCheck(func(values []int64) bool {
		max := int64(0)
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if v > max {
					max = v
				}
			}
		}
		return max == bits.MaxInt64(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxUint32(t *testing.T) {
	err := quickCheck(func(values []uint32) bool {
		max := uint32(0)
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if v > max {
					max = v
				}
			}
		}
		return max == bits.MaxUint32(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxUint64(t *testing.T) {
	err := quickCheck(func(values []uint64) bool {
		max := uint64(0)
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if v > max {
					max = v
				}
			}
		}
		return max == bits.MaxUint64(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxFloat32(t *testing.T) {
	err := quickCheck(func(values []float32) bool {
		max := float32(0)
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if v > max {
					max = v
				}
			}
		}
		return max == bits.MaxFloat32(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxFloat64(t *testing.T) {
	err := quickCheck(func(values []float64) bool {
		max := float64(0)
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if v > max {
					max = v
				}
			}
		}
		return max == bits.MaxFloat64(values)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxFixedLenByteArray1(t *testing.T) {
	err := quickCheck(func(values []byte) bool {
		max := [1]byte{}
		if len(values) > 0 {
			max[0] = values[0]
			for _, v := range values[1:] {
				if v > max[0] {
					max[0] = v
				}
			}
		}
		ret := bits.MaxFixedLenByteArray(1, values)
		return (len(values) == 0 && ret == nil) || bytes.Equal(max[:], ret)
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMaxFixedLenByteArray16(t *testing.T) {
	err := quickCheck(func(values [][16]byte) bool {
		max := [16]byte{}
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if bytes.Compare(v[:], max[:]) > 0 {
					max = v
				}
			}
		}
		ret := bits.MaxFixedLenByteArray(16, bits.Uint128ToBytes(values))
		return (len(values) == 0 && ret == nil) || bytes.Equal(max[:], ret)
	})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkMaxBool(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]bool, bufferSize/1)
		for i := 0; i < b.N; i++ {
			bits.MaxBool(values)
		}
	})
}

func BenchmarkMaxInt32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Int31()
		}
		for i := 0; i < b.N; i++ {
			bits.MaxInt32(values)
		}
	})
}

func BenchmarkMaxInt64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Int63()
		}
		for i := 0; i < b.N; i++ {
			bits.MaxInt64(values)
		}
	})
}

func BenchmarkMaxUint32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Uint32()
		}
		for i := 0; i < b.N; i++ {
			bits.MaxUint32(values)
		}
	})
}

func BenchmarkMaxUint64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Uint64()
		}
		for i := 0; i < b.N; i++ {
			bits.MaxUint64(values)
		}
	})
}

func BenchmarkMaxFloat32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Float32()
		}
		for i := 0; i < b.N; i++ {
			bits.MaxFloat32(values)
		}
	})
}

func BenchmarkMaxFloat64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Float64()
		}
		for i := 0; i < b.N; i++ {
			bits.MaxFloat64(values)
		}
	})
}

func BenchmarkMaxFixedLenByteArray(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]byte, bufferSize)
		prng := rand.New(rand.NewSource(1))
		prng.Read(values)
		for i := 0; i < b.N; i++ {
			bits.MaxFixedLenByteArray(16, values)
		}
	})
}
