package bytealg_test

import (
	"bytes"
	"testing"

	"github.com/segmentio/parquet-go/internal/bytealg"
	"github.com/segmentio/parquet-go/internal/quick"
)

func TestCount(t *testing.T) {
	err := quick.Check(func(data []byte) bool {
		data = bytes.Repeat(data, 8)
		for _, c := range data {
			n1 := bytes.Count(data, []byte{c})
			n2 := bytealg.Count(data, c)
			if n1 != n2 {
				t.Errorf("got=%d want=%d", n2, n1)
				return false
			}
		}
		return true
	})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkCount(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		data := make([]byte, bufferSize)
		for i := range data {
			data[i] = byte(i)
		}
		for i := 0; i < b.N; i++ {
			bytealg.Count(data, 0)
		}
	})
}
