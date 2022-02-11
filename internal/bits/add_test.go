package bits_test

import (
	"math"
	"testing"

	"github.com/segmentio/parquet-go/internal/bits"
)

func TestAddInt32(t *testing.T) {
	err := quickCheck(func(values []int32) bool {
		const add = math.MaxInt32

		valuesAndAdd := make([]int32, len(values))
		copy(valuesAndAdd, values)
		bits.AddInt32(valuesAndAdd, add)

		for i := range values {
			x := valuesAndAdd[i]
			y := values[i] + add
			if x != y {
				t.Errorf("unexpected value at index %d: got=%d want=%d", i, x, y)
				return false
			}
		}

		return true
	})
	if err != nil {
		t.Error(err)
	}
}

func TestAddInt64(t *testing.T) {
	err := quickCheck(func(values []int64) bool {
		const add = math.MaxInt64

		valuesAndAdd := make([]int64, len(values))
		copy(valuesAndAdd, values)
		bits.AddInt64(valuesAndAdd, add)

		for i := range values {
			x := valuesAndAdd[i]
			y := values[i] + add
			if x != y {
				t.Errorf("unexpected value at index %d: got=%d want=%d", i, x, y)
				return false
			}
		}

		return true
	})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkAddInt32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int32, bufferSize/4)
		for i := 0; i < b.N; i++ {
			bits.AddInt32(values, 1)
		}
	})
}

func BenchmarkAddInt64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int64, bufferSize/8)
		for i := 0; i < b.N; i++ {
			bits.AddInt64(values, 1)
		}
	})
}
