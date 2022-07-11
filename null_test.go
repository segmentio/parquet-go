//go:build go1.18

package parquet

import (
	"reflect"
	"testing"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/internal/quick"
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
	testNullIndex[string](t)
	testNullIndex[*struct{}](t)
}

func testNullIndex[T comparable](t *testing.T) {
	var zero T
	t.Helper()
	t.Run(reflect.TypeOf(zero).String(), func(t *testing.T) {
		err := quick.Check(func(data []T) bool {
			if len(data) == 0 {
				return true
			}

			want := make([]uint64, (len(data)+63)/64)
			got := make([]uint64, (len(data)+63)/64)

			for i := range data {
				if (i % 2) == 0 {
					data[i] = zero
				}
			}

			array := makeArrayOf(data)
			nullIndex[T](want, array)
			nullIndexFuncOf(reflect.TypeOf(zero))(got, array)

			if !reflect.DeepEqual(want, got) {
				t.Errorf("unexpected null index\nwant = %064b\ngot  = %064b", want, got)
				return false
			}
			return true
		})
		if err != nil {
			t.Error(err)
		}
	})
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
	benchmarkNullIndex[string](b)
	benchmarkNullIndex[[]struct{}](b)
	benchmarkNullIndex[*struct{}](b)
}

func benchmarkNullIndex[T any](b *testing.B) {
	const N = 1000

	var zero T
	typ := reflect.TypeOf(zero)
	null := nullIndexFuncOf(typ)
	data := makeArrayOf(make([]T, N))
	bits := make([]uint64, (N+63)/64)

	b.Run(typ.String(), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			null(bits, data)
		}
		b.SetBytes(int64(typ.Size() * N))
	})
}
