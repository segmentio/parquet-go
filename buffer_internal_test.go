package parquet

import (
	"math"
	"math/rand"
	"testing"
)

func TestBufferAlwaysCorrectSize(t *testing.T) {
	var p bufferPool
	for i := 0; i < 1000; i++ {
		sz := rand.Intn(1024 * 1024)
		buff := p.get(sz)
		if len(buff.data) != sz {
			t.Errorf("Expected buffer of size %d, got %d", sz, len(buff.data))
		}
		p.put(buff)
	}
}

func TestLevelledPoolIndex(t *testing.T) {
	tcs := []struct {
		sz       int
		expected int
	}{
		{
			sz:       1023,
			expected: 0,
		},
		{
			sz:       1024,
			expected: 1,
		},
		{
			sz:       -1,
			expected: 0,
		},
		{
			sz:       16*1024*1024 - 1,
			expected: 14,
		},
		{
			sz:       16 * 1024 * 1024,
			expected: 15,
		},
		{
			sz:       math.MaxInt,
			expected: 15,
		},
	}

	for _, tc := range tcs {
		if actual := levelledPoolIndex(tc.sz); actual != tc.expected {
			t.Errorf("Expected index %d for size %d, got %d", tc.expected, tc.sz, actual)
		}
	}
}
