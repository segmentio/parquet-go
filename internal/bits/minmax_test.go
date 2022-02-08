package bits_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/segmentio/parquet-go/internal/bits"
)

const scaleFactor = 6000

func TestMinMaxBool(t *testing.T) {
	err := quickCheck(func(values []bool) bool {
		min := len(values) > 0
		max := false
		for _, v := range values {
			if v {
				max = true
			} else {
				min = false
			}
		}
		minValue, maxValue := bits.MinMaxBool(values)
		return min == minValue && max == maxValue
	})
	if err != nil {
		t.Error(err)
	}

	values := make([]bool, 200)
	if minValue, maxValue := bits.MinMaxBool(values); minValue || maxValue {
		t.Error("min and max values must be false when all input values are false")
	}
	for i := range values {
		values[i] = true
	}
	if minValue, maxValue := bits.MinMaxBool(values); !minValue || !maxValue {
		t.Error("min and max values must be true when all input values are true")
	}
}

func TestMinMaxInt32(t *testing.T) {
	err := quickCheck(func(values []int32) bool {
		min := int32(0)
		max := int32(0)
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
		}
		minValue, maxValue := bits.MinMaxInt32(values)
		return min == minValue && max == maxValue
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinMaxInt64(t *testing.T) {
	err := quickCheck(func(values []int64) bool {
		min := int64(0)
		max := int64(0)
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
		}
		minValue, maxValue := bits.MinMaxInt64(values)
		return min == minValue && max == maxValue
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinMaxUint32(t *testing.T) {
	err := quickCheck(func(values []uint32) bool {
		min := uint32(0)
		max := uint32(0)
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
		}
		minValue, maxValue := bits.MinMaxUint32(values)
		return min == minValue && max == maxValue
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinMaxUint64(t *testing.T) {
	err := quickCheck(func(values []uint64) bool {
		min := uint64(0)
		max := uint64(0)
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
		}
		minValue, maxValue := bits.MinMaxUint64(values)
		return min == minValue && max == maxValue
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinMaxFloat32(t *testing.T) {
	err := quickCheck(func(values []float32) bool {
		min := float32(0)
		max := float32(0)
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
		}
		minValue, maxValue := bits.MinMaxFloat32(values)
		return min == minValue && max == maxValue
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinMaxFloat64(t *testing.T) {
	err := quickCheck(func(values []float64) bool {
		min := float64(0)
		max := float64(0)
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
		}
		minValue, maxValue := bits.MinMaxFloat64(values)
		return min == minValue && max == maxValue
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinMaxFixedLenByteArray1(t *testing.T) {
	err := quickCheck(func(values []byte) bool {
		min := [1]byte{}
		max := [1]byte{}
		if len(values) > 0 {
			min[0] = values[0]
			max[0] = values[0]
			for _, v := range values[1:] {
				if v < min[0] {
					min[0] = v
				}
				if v > max[0] {
					max[0] = v
				}
			}
		}
		minValue, maxValue := bits.MinMaxFixedLenByteArray(1, values)
		return (len(values) == 0 && minValue == nil && maxValue == nil) ||
			(bytes.Equal(min[:], minValue) && bytes.Equal(max[:], maxValue))
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMinMaxFixedLenByteArray16(t *testing.T) {
	err := quickCheck(func(values [][16]byte) bool {
		min := [16]byte{}
		max := [16]byte{}
		if len(values) > 0 {
			min = values[0]
			max = values[0]
			for _, v := range values[1:] {
				if bytes.Compare(v[:], min[:]) < 0 {
					min = v
				}
				if bytes.Compare(v[:], max[:]) > 0 {
					max = v
				}
			}
		}
		minValue, maxValue := bits.MinMaxFixedLenByteArray(16, bits.Uint128ToBytes(values))
		return (len(values) == 0 && minValue == nil && maxValue == nil) ||
			(bytes.Equal(min[:], minValue) && bytes.Equal(max[:], maxValue))
	})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkMinMaxBool(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]bool, bufferSize/1)
		for i := range values {
			values[i] = true
		}
		for i := 0; i < b.N; i++ {
			bits.MinMaxBool(values)
		}
	})
}

func BenchmarkMinMaxInt32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Int31()
		}
		for i := 0; i < b.N; i++ {
			bits.MinMaxInt32(values)
		}
	})
}

func BenchmarkMinMaxInt64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Int63()
		}
		for i := 0; i < b.N; i++ {
			bits.MinMaxInt64(values)
		}
	})
}

func BenchmarkMinMaxUint32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Uint32()
		}
		for i := 0; i < b.N; i++ {
			bits.MinMaxUint32(values)
		}
	})
}

func BenchmarkMinMaxUint64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Uint64()
		}
		for i := 0; i < b.N; i++ {
			bits.MinMaxUint64(values)
		}
	})
}

func BenchmarkMinMaxFloat32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float32, bufferSize/4)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Float32()
		}
		for i := 0; i < b.N; i++ {
			bits.MinMaxFloat32(values)
		}
	})
}

func BenchmarkMinMaxFloat64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float64, bufferSize/8)
		prng := rand.New(rand.NewSource(1))
		for i := range values {
			values[i] = prng.Float64()
		}
		for i := 0; i < b.N; i++ {
			bits.MinMaxFloat64(values)
		}
	})
}

func BenchmarkMinMaxFixedLenByteArray(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]byte, bufferSize)
		prng := rand.New(rand.NewSource(1))
		prng.Read(values)
		for i := 0; i < b.N; i++ {
			bits.MinMaxFixedLenByteArray(16, values)
		}
	})
}
