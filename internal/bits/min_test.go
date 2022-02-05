package bits_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/segmentio/parquet-go/internal/bits"
)

func TestMinBool(t *testing.T) {
	f := func(values []bool) bool {
		min := len(values) > 0
		for _, v := range values {
			if !v {
				min = false
				break
			}
		}
		return min == bits.MinBool(values)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMinInt32(t *testing.T) {
	f := func(values []int32) bool {
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMinInt64(t *testing.T) {
	f := func(values []int64) bool {
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMinUint32(t *testing.T) {
	f := func(values []uint32) bool {
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMinUint64(t *testing.T) {
	f := func(values []uint64) bool {
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMinFloat32(t *testing.T) {
	f := func(values []float32) bool {
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMinFloat64(t *testing.T) {
	f := func(values []float64) bool {
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMinFixedLenByteArray1(t *testing.T) {
	f := func(values []byte) bool {
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
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestMinFixedLenByteArray16(t *testing.T) {
	f := func(values [][16]byte) bool {
		min := [16]byte{}
		if len(values) > 0 {
			min = values[0]
			for _, v := range values[1:] {
				if bytes.Compare(v[:], min[:]) < 0 {
					min = v
				}
			}
		}
		// Increase the size of the input to make sure we are exercising the
		// vectorized code paths.
		data := bytes.Repeat(bits.Uint128ToBytes(values), 4)
		ret := bits.MinFixedLenByteArray(16, data)
		return (len(values) == 0 && ret == nil) || bytes.Equal(min[:], ret)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func BenchmarkMinBool(b *testing.B) {
	values := make([]bool, bufferSize/1)
	for i := range values {
		values[i] = true
	}
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MinBool(values)
		}
	})
}

func BenchmarkMinInt32(b *testing.B) {
	values := make([]int32, bufferSize/4)
	prng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = prng.Int31()
	}
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MinInt32(values)
		}
	})
}

func BenchmarkMinInt64(b *testing.B) {
	values := make([]int64, bufferSize/8)
	prng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = prng.Int63()
	}
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MinInt64(values)
		}
	})
}

func BenchmarkMinUint32(b *testing.B) {
	values := make([]uint32, bufferSize/4)
	prng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = prng.Uint32()
	}
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MinUint32(values)
		}
	})
}

func BenchmarkMinUint64(b *testing.B) {
	values := make([]uint64, bufferSize/8)
	prng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = prng.Uint64()
	}
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MinUint64(values)
		}
	})
}

func BenchmarkMinFloat32(b *testing.B) {
	values := make([]float32, bufferSize/4)
	prng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = prng.Float32()
	}
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MinFloat32(values)
		}
	})
}

func BenchmarkMinFloat64(b *testing.B) {
	values := make([]float64, bufferSize/8)
	prng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = prng.Float64()
	}
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MinFloat64(values)
		}
	})
}

func BenchmarkMinFixedLenByteArray(b *testing.B) {
	values := make([]byte, bufferSize)
	prng := rand.New(rand.NewSource(1))
	prng.Read(values)
	b.Run(fmt.Sprintf("%dKB", bufferSize/1024), func(b *testing.B) {
		b.SetBytes(bufferSize)
		for i := 0; i < b.N; i++ {
			bits.MinFixedLenByteArray(16, values)
		}
	})
}
