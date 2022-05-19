package parquet_test

import (
	"io"
	"math/rand"
	"testing"

	"github.com/segmentio/parquet-go"
)

func BenchmarkGenericWriter(b *testing.B) {
	data := make([]byte, 16*benchmarkReaderNumRows)
	prng := rand.New(rand.NewSource(0))
	prng.Read(data)

	values := make([]benchmarkBufferRowType, benchmarkReaderNumRows)
	for i := range values {
		j := (i + 0) * 16
		k := (i + 1) * 16
		copy(values[i].ID[:], data[j:k])
		values[i].Value = prng.Float64()
	}

	b.Run("go1.17", func(b *testing.B) {
		writer := parquet.NewWriter(io.Discard, parquet.SchemaOf(values[0]))
		i := 0
		benchmarkRowsPerSecond(b, func() int {
			for j := 0; j < benchmarkBufferRowsPerStep; j++ {
				if err := writer.Write(&values[i]); err != nil {
					b.Fatal(err)
				}
			}

			i += benchmarkBufferRowsPerStep
			i %= benchmarkBufferNumRows

			if i == 0 {
				writer.Close()
				writer.Reset(io.Discard)
			}
			return benchmarkBufferRowsPerStep
		})
	})

	b.Run("go1.18", func(b *testing.B) {
		writer := parquet.NewGenericWriter[benchmarkBufferRowType](io.Discard)
		i := 0
		benchmarkRowsPerSecond(b, func() int {
			n, err := writer.Write(values[i : i+benchmarkBufferRowsPerStep])
			if err != nil {
				b.Fatal(err)
			}

			i += benchmarkBufferRowsPerStep
			i %= benchmarkBufferNumRows

			if i == 0 {
				writer.Close()
				writer.Reset(io.Discard)
			}
			return n
		})
	})
}
