package parquet_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/segmentio/parquet-go"
)

func BenchmarkDictionary(b *testing.B) {
	const N = 1000

	types := []parquet.Type{
		parquet.BooleanType,
		parquet.Int32Type,
		parquet.Int64Type,
		parquet.Int96Type,
		parquet.FloatType,
		parquet.DoubleType,
		parquet.ByteArrayType,
	}

	tests := []struct {
		scenario string
		init     func(parquet.Dictionary, []int32, []parquet.Value)
		test     func(parquet.Dictionary, []int32, []parquet.Value)
	}{
		{
			scenario: "Bounds",
			init:     parquet.Dictionary.Insert,
			test: func(dict parquet.Dictionary, indexes []int32, _ []parquet.Value) {
				dict.Bounds(indexes)
			},
		},

		{
			scenario: "Insert",
			test:     parquet.Dictionary.Insert,
		},

		{
			scenario: "Lookup",
			init:     parquet.Dictionary.Insert,
			test:     parquet.Dictionary.Lookup,
		},
	}

	for _, test := range tests {
		b.Run(test.scenario, func(b *testing.B) {
			for _, typ := range types {
				dict := typ.NewDictionary(0, 0, make([]byte, 0, 4*N))
				values := make([]parquet.Value, N)

				f := randValueFuncOf(typ)
				r := rand.New(rand.NewSource(0))

				for i := range values {
					values[i] = f(r)
				}

				indexes := make([]int32, len(values))
				if test.init != nil {
					test.init(dict, indexes, values)
				}

				b.Run(typ.String(), func(b *testing.B) {
					start := time.Now()

					for i := 0; i < b.N; i++ {
						test.test(dict, indexes, values)
					}

					seconds := time.Since(start).Seconds()
					b.ReportMetric(float64(b.N)/seconds, "value/s")
				})
			}
		})
	}
}
