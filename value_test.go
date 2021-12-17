package parquet_test

import (
	"testing"
	"unsafe"

	"github.com/segmentio/parquet"
)

func TestSizeOfValue(t *testing.T) {
	t.Logf("sizeof(parquet.Value) = %d", unsafe.Sizeof(parquet.Value{}))
}

func BenchmarkValueAppend(b *testing.B) {
	const N = 1024
	row := make(parquet.Row, 0, N)
	val := parquet.ValueOf(42)

	for i := 0; i < b.N; i++ {
		row = row[:0]
		for j := 0; j < N; j++ {
			row = append(row, val)
		}
	}

	b.SetBytes(N * int64(unsafe.Sizeof(parquet.Value{})))
}
