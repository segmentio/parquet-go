package compact_test

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/compact"
)

func TestFixedIntArray(t *testing.T) {
	for _, bitWidth := range []int{8, 10, 16, 24, 32} {
		t.Run(strconv.Itoa(bitWidth)+"bits", func(t *testing.T) {
			testFixedArray(t, bitWidth, compact.NewFixedIntArray(bitWidth))
		})
	}
}

func TestDynamicIntArray(t *testing.T) {
	for _, bitWidth := range []int{8, 10, 16, 24, 32} {
		t.Run(strconv.Itoa(bitWidth)+"bits", func(t *testing.T) {
			testFixedArray(t, bitWidth, compact.NewDynamicIntArray())
		})
	}
}

func testFixedArray(t *testing.T, bitWidth int, array encoding.IntArray) {
	if n := array.Len(); n != 0 {
		t.Errorf("new array must have a zero length but got %d", n)
	}

	const N = 10
	prng := rand.New(rand.NewSource(0))
	maxInt := int64(1 << bitWidth)
	minInt := -((maxInt / 2) + 1)

	for i := 0; i < N; i++ {
		array.Append(prng.Int63n(maxInt) + minInt)

		if n := array.Len(); n != (i + 1) {
			t.Errorf("wrong array length: want=%d got=%d", i+1, n)
		}
	}

	prng.Seed(0)

	for i := 0; i < N; i++ {
		u := prng.Int63n(maxInt) + minInt
		v := array.Index(i)

		if u != v {
			t.Errorf("wrong array value at index %d: want=%d got=%d", i, u, v)
		}
	}

	array.Reset()
	if n := array.Len(); n != 0 {
		t.Errorf("after resetting the array length must be zero but got %d", n)
	}
}
