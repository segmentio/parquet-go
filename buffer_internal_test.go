package parquet

import (
	"fmt"
	"math/rand"
	"testing"
)

func init() {
	size := bufferPoolMinSize
	for i := 0; i < bufferPoolBucketCount; i++ {
		fmt.Printf("%d KiB\n", size/1024)
		size = bufferPoolNextSize(size)
	}
}

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

/*
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
			get:  0,
			put:  0,
		},

		{
			size: 256*1024 - 1,
			get:  8,
			put:  8,
		},

		{
			size: 256 * 1024,
			get:  8,
			put:  8,
		},

		{
			size: 256*1024 + 1,
			get:  9,
			put:  9,
		},

		{
			size: 1024 * 1024,
			get:  10,
			put:  10,
		},

		{
			size: 16*1024*1024 - 1,
			get:  14,
			put:  13,
		},

		{
			size: 16 * 1024 * 1024,
			get:  14,
			put:  14,
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

func TestBufferPoolBucketSize(t *testing.T) {
	tests := []struct {
		bucket int
		size   int
	}{
		{
			bucket: 0,
			size:   1024,
		},

		{
			bucket: 1,
			size:   2048,
		},

		{
			bucket: 16,
			size:   67108864,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("bucket=%d", test.bucket), func(t *testing.T) {
			if size := bufferPoolBucketSize(test.bucket); size != test.size {
				t.Errorf("bucket %d: want=%d got=%d", test.bucket, test.size, size)
			}
		})
	}

	for i := 0; i < bufferPoolBucketCount; i++ {
		n := bufferPoolBucketSize(i)
		j := bufferPoolBucketIndexGet(n)
		k := bufferPoolBucketIndexPut(n + 1)
		if i != j {
			t.Errorf("index for size=%d is %d but want %d", n, j, i)
		}
		if i != k {
			t.Errorf("index for size=%d is %d but want %d", n, k, i)
		}
	}
}
*/
