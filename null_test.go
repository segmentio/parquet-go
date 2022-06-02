//go:build go1.18

package parquet

import (
	"io"
	"math/rand"
	"reflect"
	"testing"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

func TestNullIndex(t *testing.T) {
	testNullIndex[bool](t)
	testNullIndex[int](t)
	testNullIndex[int32](t)
	testNullIndex[int64](t)
	testNullIndex[uint](t)
	testNullIndex[uint32](t)
	testNullIndex[uint64](t)
	testNullIndex[float32](t)
	testNullIndex[float64](t)
	testNullIndex[[10]byte](t)
	testNullIndex[[16]byte](t)
	testNullIndex[deprecated.Int96](t)
}

func testNullIndex[T comparable](t *testing.T) {
	var zero T
	var typ = reflect.TypeOf(zero)
	t.Helper()
	t.Run(typ.String(), func(t *testing.T) {
		data := make([]byte, 1023)
		prng := rand.New(rand.NewSource(0))

		for seed := int64(0); seed < 10; seed++ {
			prng.Seed(seed)
			io.ReadFull(prng, data)

			array := makeArray(unsafecast.Slice[T](data))
			want := nullIndex[T](array)
			got := nullIndexFuncOf(typ)(array)

			if want != got {
				t.Errorf("unexpected null index: want=%d got=%d", want, got)
			}
		}
	})
}

func nullIndex[T comparable](a array) int {
	var zero T

	for i, v := range makeSlice[T](a) {
		if v == zero {
			return i
		}
	}

	return a.len
}

func BenchmarkNullIndex(b *testing.B) {
	benchmarkNullIndex[bool](b)
	benchmarkNullIndex[int](b)
	benchmarkNullIndex[int32](b)
	benchmarkNullIndex[int64](b)
	benchmarkNullIndex[uint](b)
	benchmarkNullIndex[uint32](b)
	benchmarkNullIndex[uint64](b)
	benchmarkNullIndex[float32](b)
	benchmarkNullIndex[float64](b)
	benchmarkNullIndex[[10]byte](b)
	benchmarkNullIndex[[16]byte](b)
	benchmarkNullIndex[deprecated.Int96](b)
}

func benchmarkNullIndex[T comparable](b *testing.B) {
	var zero T
	typ := reflect.TypeOf(zero)
	nullIndex := nullIndexFuncOf(typ)

	data := make([]byte, 1023)
	for i := range data {
		data[i] = 0xFF
	}

	clear := data[len(data)-20:]
	for i := range clear {
		clear[i] = 0
	}

	b.Run(typ.String(), func(b *testing.B) {
		a := makeArray(unsafecast.Slice[T](data))
		j := 0

		for i := 0; i < b.N; i++ {
			j = nullIndex(a)
		}

		b.SetBytes(int64(j) * int64(typ.Size()))
	})
}
