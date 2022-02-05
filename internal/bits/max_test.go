package bits_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/segmentio/parquet-go/internal/bits"
)

func TestMaxBool(t *testing.T) {
	f := func(values []bool) bool {
		values = repeatBool(values, 4)
		max := false
		for _, v := range values {
			if v {
				max = true
				break
			}
		}
		return max == bits.MaxBool(values)
	}
	if err := quick.Check(f, nil); err != nil {
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
	f := func(values []int32) bool {
		values = repeatInt32(values, 4)
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMaxInt64(t *testing.T) {
	f := func(values []int64) bool {
		values = repeatInt64(values, 4)
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMaxUint32(t *testing.T) {
	f := func(values []uint32) bool {
		values = repeatUint32(values, 4)
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMaxUint64(t *testing.T) {
	f := func(values []uint64) bool {
		values = repeatUint64(values, 4)
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMaxFloat32(t *testing.T) {
	f := func(values []float32) bool {
		values = repeatFloat32(values, 4)
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMaxFloat64(t *testing.T) {
	f := func(values []float64) bool {
		values = repeatFloat64(values, 4)
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMaxFixedLenByteArray1(t *testing.T) {
	f := func(values []byte) bool {
		values = bytes.Repeat(values, 4)
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMaxFixedLenByteArray16(t *testing.T) {
	f := func(values [][16]byte) bool {
		max := [16]byte{}
		if len(values) > 0 {
			max = values[0]
			for _, v := range values[1:] {
				if bytes.Compare(v[:], max[:]) > 0 {
					max = v
				}
			}
		}
		// Increase the size of the input to make sure we are exercising the
		// vectorized code paths.
		data := bytes.Repeat(bits.Uint128ToBytes(values), 4)
		ret := bits.MaxFixedLenByteArray(16, data)
		return (len(values) == 0 && ret == nil) || bytes.Equal(max[:], ret)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func BenchmarkMaxBool(b *testing.B) {
	values := make([]bool, bufferSize/1)
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MaxBool(values)
		}
	})
}

func BenchmarkMaxInt32(b *testing.B) {
	values := make([]int32, bufferSize/4)
	prng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = prng.Int31()
	}
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MaxInt32(values)
		}
	})
}

func BenchmarkMaxInt64(b *testing.B) {
	values := make([]int64, bufferSize/8)
	prng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = prng.Int63()
	}
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MaxInt64(values)
		}
	})
}

func BenchmarkMaxUint32(b *testing.B) {
	values := make([]uint32, bufferSize/4)
	prng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = prng.Uint32()
	}
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MaxUint32(values)
		}
	})
}

func BenchmarkMaxUint64(b *testing.B) {
	values := make([]uint64, bufferSize/8)
	prng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = prng.Uint64()
	}
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MaxUint64(values)
		}
	})
}

func BenchmarkMaxFloat32(b *testing.B) {
	values := make([]float32, bufferSize/4)
	prng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = prng.Float32()
	}
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MaxFloat32(values)
		}
	})
}

func BenchmarkMaxFloat64(b *testing.B) {
	values := make([]float64, bufferSize/8)
	prng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = prng.Float64()
	}
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MaxFloat64(values)
		}
	})
}

func BenchmarkMaxFixedLenByteArray(b *testing.B) {
	values := make([]byte, bufferSize)
	prng := rand.New(rand.NewSource(1))
	prng.Read(values)
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MaxFixedLenByteArray(16, values)
		}
	})
}
