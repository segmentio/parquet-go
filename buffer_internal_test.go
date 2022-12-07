package parquet

import (
	"math"
	"math/rand"
	"testing"
)

func TestBufferAlwaysCorrectSize(t *testing.T) {
	var p bufferPool
	for i := 0; i < 1000; i++ {
		n := rand.Intn(1024 * 1024)
		b := p.get(n)
		if len(b.data) != n {
			t.Errorf("Expected buffer of size %d, got %d", n, len(b.data))
		}
		b.unref()
	}
}

func TestBufferPoolBucketIndex(t *testing.T) {
	tcs := []struct {
		size     int
		expected int
	}{
		{
			size:     1023,
			expected: 0,
		},

		{
			size:     1024,
			expected: 1,
		},

		{
			size:     256 * 1024,
			expected: 9,
		},

		{
			size:     -1,
			expected: 0,
		},

		{
			size:     16*1024*1024 - 1,
			expected: 14,
		},

		{
			size:     16 * 1024 * 1024,
			expected: 15,
		},

		{
			size:     math.MaxInt,
			expected: 15,
		},
	}

	for _, tc := range tcs {
		if actual := bufferPoolBucketIndex(tc.size); actual != tc.expected {
			t.Errorf("expected index %d when acquiring buffer of size %d, got %d", tc.expected, tc.size, actual)
		}
	}
}
