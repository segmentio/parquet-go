package bits_test

import (
	"math"
	"testing"

	"github.com/segmentio/parquet-go/internal/bits"
)

func TestSubInt32(t *testing.T) {
	err := quickCheck(func(values []int32) bool {
		const sub = math.MaxInt32

		valuesAndSub := make([]int32, len(values))
		copy(valuesAndSub, values)
		bits.SubInt32(valuesAndSub, sub)

		for i := range values {
			x := valuesAndSub[i]
			y := values[i] - sub
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

func TestSubInt64(t *testing.T) {
	err := quickCheck(func(values []int64) bool {
		const sub = math.MaxInt64

		valuesAndSub := make([]int64, len(values))
		copy(valuesAndSub, values)
		bits.SubInt64(valuesAndSub, sub)

		for i := range values {
			x := valuesAndSub[i]
			y := values[i] - sub
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

func BenchmarkSubInt32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int32, bufferSize/4)
		for i := 0; i < b.N; i++ {
			bits.SubInt32(values, 1)
		}
	})
}

func BenchmarkSubInt64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int64, bufferSize/8)
		for i := 0; i < b.N; i++ {
			bits.SubInt64(values, 1)
		}
	})
}
