package parquet_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/segmentio/parquet-go"
)

var dictionaryTypes = [...]parquet.Type{
	parquet.BooleanType,
	parquet.Int32Type,
	parquet.Int64Type,
	parquet.Int96Type,
	parquet.FloatType,
	parquet.DoubleType,
	parquet.ByteArrayType,
	parquet.FixedLenByteArrayType(16),
}

func TestDictionary(t *testing.T) {
	for _, typ := range dictionaryTypes {
		t.Run(typ.String(), func(t *testing.T) {
			testDictionary(t, typ)
		})
	}
}

func testDictionary(t *testing.T, typ parquet.Type) {
	const N = 500

	dict := typ.NewDictionary(0, 0, nil)
	values := make([]parquet.Value, N)
	indexes := make([]int32, N)
	lookups := make([]parquet.Value, N)

	f := randValueFuncOf(typ)
	r := rand.New(rand.NewSource(0))

	for i := range values {
		values[i] = f(r)
	}

	mapping := make(map[int32]parquet.Value, N)

	for i := 0; i < N; {
		j := i + ((N-i)/2 + 1)
		if j > N {
			j = N
		}

		dict.Insert(indexes[i:j], values[i:j])

		for k, v := range values[i:j] {
			mapping[indexes[i+k]] = v
		}

		for _, index := range indexes[i:j] {
			if index < 0 || index >= int32(dict.Len()) {
				t.Fatalf("index out of bounds: %d", index)
			}
		}

		r.Shuffle(j-i, func(a, b int) {
			indexes[a+i], indexes[b+i] = indexes[b+i], indexes[a+i]
		})

		dict.Lookup(indexes[i:j], lookups[i:j])

		for lookupIndex, valueIndex := range indexes[i:j] {
			want := mapping[valueIndex]
			got := lookups[lookupIndex+i]

			if !parquet.Equal(want, got) {
				t.Fatalf("wrong value looked up at index %d: want=%#v got=%#v", valueIndex, want, got)
			}
		}

		i = j
	}

	for i := range lookups {
		lookups[i] = parquet.Value{}
	}

	dict.Lookup(indexes, lookups)

	for lookupIndex, valueIndex := range indexes {
		want := mapping[valueIndex]
		got := lookups[lookupIndex]

		if !parquet.Equal(want, got) {
			t.Fatalf("wrong value looked up at index %d: want=%+v got=%+v", valueIndex, want, got)
		}
	}
}

func BenchmarkDictionary(b *testing.B) {
	const N = 1000

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
			for _, typ := range dictionaryTypes {
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
					b.ReportMetric(float64(N*b.N)/seconds, "value/s")
				})
			}
		})
	}
}
