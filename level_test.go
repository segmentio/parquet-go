package parquet

import (
	"fmt"
	"testing"
)

func TestMemset(t *testing.T) {
	const N = 100_0000
	buffer := make([]byte, N)

	for n := 1; n <= N; n = (n * 2) + 1 {
		t.Run(fmt.Sprintf("size=%d", n), func(t *testing.T) {
			b := buffer[:n]

			for i := range b {
				b[i] = 0
			}

			memset(b, 42)

			for i, c := range b {
				if c != 42 {
					t.Fatalf("byte at index %d has value %d", i, c)
				}
			}
		})
	}
}

func BenchmarkMemset(b *testing.B) {
	for _, size := range []int{0, 10, 100, 1000, 10_000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			data := make([]byte, size)

			for i := 0; i < b.N; i++ {
				memset(data, 1)
			}

			b.SetBytes(int64(size))
		})
	}
}
