package parquet_test

import (
	"math/rand"
	"testing"

	"github.com/segmentio/parquet-go"
)

func BenchmarkDictionaryPageBounds(b *testing.B) {
	benchmarkDictionary(b, func(b *testing.B, typ parquet.Type) {
		values := make([]parquet.Value, 1e3)
		f := randValueFuncOf(typ)
		r := rand.New(rand.NewSource(0))

		for i := range values {
			values[i] = f(r)
		}

		for i := 0; i < b.N; i++ {
			page := randDictionaryPage(typ, values)
			page.Bounds()
		}
	})
}

func benchmarkDictionary(b *testing.B, do func(*testing.B, parquet.Type)) {
	types := []parquet.Type{
		parquet.BooleanType,
		parquet.Int32Type,
		parquet.Int64Type,
		parquet.Int96Type,
		parquet.FloatType,
		parquet.DoubleType,
		parquet.ByteArrayType,
	}

	for _, typ := range types {
		b.Run(typ.String(), func(b *testing.B) { do(b, typ) })
	}
}

func randDictionaryPage(typ parquet.Type, values []parquet.Value) parquet.BufferedPage {
	const bufferSize = 64 * 1024
	dict := typ.NewDictionary(4 * bufferSize)
	buf := dict.Type().NewColumnBuffer(0, bufferSize)
	buf.WriteValues(values)
	return buf.Page()
}
