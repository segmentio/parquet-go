package parquet_test

import (
	"fmt"
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
	parquet.FixedLenByteArrayType(10),
	parquet.FixedLenByteArrayType(16),
	parquet.Uint(32).Type(),
	parquet.Uint(64).Type(),
}

func TestDictionary(t *testing.T) {
	for _, typ := range dictionaryTypes {
		for _, numValues := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1e2, 1e3, 1e4} {
			t.Run(fmt.Sprintf("%s/N=%d", typ, numValues), func(t *testing.T) {
				testDictionary(t, typ, numValues)
			})
		}
	}
}

func testDictionary(t *testing.T, typ parquet.Type, numValues int) {
	const columnIndex = 1

	dict := typ.NewDictionary(columnIndex, 0, nil)
	values := make([]parquet.Value, numValues)
	indexes := make([]int32, numValues)
	lookups := make([]parquet.Value, numValues)

	f := randValueFuncOf(typ)
	r := rand.New(rand.NewSource(int64(numValues)))

	for i := range values {
		values[i] = f(r)
		values[i] = values[i].Level(0, 0, columnIndex)
	}

	mapping := make(map[int32]parquet.Value, numValues)

	for i := 0; i < numValues; {
		j := i + ((numValues-i)/2 + 1)
		if j > numValues {
			j = numValues
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

			if !parquet.DeepEqual(want, got) {
				t.Fatalf("wrong value looked up at index %d: want=%#v got=%#v", valueIndex, want, got)
			}
		}

		minValue := values[i]
		maxValue := values[i]

		for _, value := range values[i+1 : j] {
			switch {
			case typ.Compare(value, minValue) < 0:
				minValue = value
			case typ.Compare(value, maxValue) > 0:
				maxValue = value
			}
		}

		lowerBound, upperBound := dict.Bounds(indexes[i:j])
		if !parquet.DeepEqual(lowerBound, minValue) {
			t.Errorf("wrong lower bound betwen indexes %d and %d: want=%#v got=%#v", i, j, minValue, lowerBound)
		}
		if !parquet.DeepEqual(upperBound, maxValue) {
			t.Errorf("wrong upper bound between indexes %d and %d: want=%#v got=%#v", i, j, maxValue, upperBound)
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

	for i, test := range tests {
		b.Run(test.scenario, func(b *testing.B) {
			for j, typ := range dictionaryTypes {
				for _, numValues := range []int{1e2, 1e3, 1e4, 1e5, 1e6} {
					dict := typ.NewDictionary(0, 0, make([]byte, 0, 4*numValues))
					values := make([]parquet.Value, numValues)

					f := randValueFuncOf(typ)
					r := rand.New(rand.NewSource(int64(i * j * numValues)))

					for i := range values {
						values[i] = f(r)
					}

					indexes := make([]int32, len(values))
					if test.init != nil {
						test.init(dict, indexes, values)
					}

					b.Run(fmt.Sprintf("%s/N=%d", typ, numValues), func(b *testing.B) {
						start := time.Now()

						for i := 0; i < b.N; i++ {
							test.test(dict, indexes, values)
						}

						seconds := time.Since(start).Seconds()
						b.ReportMetric(float64(numValues*b.N)/seconds, "value/s")
					})
				}
			}
		})
	}
}
