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
			t.Fatalf("Expected buffer of size %d, got %d", n, len(b.data))
		}
		b.unref()
	}
}

func TestBufferPoolBucketIndex(t *testing.T) {
	tcs := []struct {
		size int
		get  int
		put  int
	}{
		{
			size: 1023,
			get:  0,
			put:  0,
		},

		{
			size: 1024,
			get:  1,
			put:  1,
		},

		{
			size: 256 * 1024,
			get:  9,
			put:  9,
		},

		{
			size: 1024 * 1024,
			get:  11,
			put:  11,
		},

		{
			size: 16*1024*1024 - 1,
			get:  15,
			put:  14,
		},

		{
			size: 16 * 1024 * 1024,
			get:  15,
			put:  15,
		},

		{
			size: math.MaxInt,
			get:  15,
			put:  15,
		},
	}

	for _, tc := range tcs {
		if index := bufferPoolBucketIndexGet(tc.size); index != tc.get {
			t.Errorf("expected index %d when acquiring buffer of size %d, got %d", tc.get, tc.size, index)
		}
		if index := bufferPoolBucketIndexPut(tc.size); index != tc.put {
			t.Errorf("expected index %d when releasing buffer of size %d, got %d", tc.put, tc.size, index)
		}
	}
}
