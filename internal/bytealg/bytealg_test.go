package bytealg_test

import (
	"fmt"
	"testing"
)

var benchmarkBufferSizes = [...]int{
	4 * 1024,
	256 * 1024,
	2048 * 1024,
}

func forEachBenchmarkBufferSize(b *testing.B, f func(*testing.B, int)) {
	for _, bufferSize := range benchmarkBufferSizes {
		b.Run(fmt.Sprintf("%dKiB", bufferSize/1024), func(b *testing.B) {
			b.SetBytes(int64(bufferSize))
			f(b, bufferSize)
		})
	}
}
