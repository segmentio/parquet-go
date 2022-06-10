package parquet

import (
	"bytes"
	"sort"
	"testing"

	"github.com/segmentio/parquet-go/internal/quick"
)

type boolOrder []bool

func (v boolOrder) Len() int           { return len(v) }
func (v boolOrder) Less(i, j int) bool { return !v[i] && v[j] }
func (v boolOrder) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }

type int32Order []int32

func (v int32Order) Len() int           { return len(v) }
func (v int32Order) Less(i, j int) bool { return v[i] < v[j] }
func (v int32Order) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }

type int64Order []int64

func (v int64Order) Len() int           { return len(v) }
func (v int64Order) Less(i, j int) bool { return v[i] < v[j] }
func (v int64Order) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }

type uint32Order []uint32

func (v uint32Order) Len() int           { return len(v) }
func (v uint32Order) Less(i, j int) bool { return v[i] < v[j] }
func (v uint32Order) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }

type uint64Order []uint64

func (v uint64Order) Len() int           { return len(v) }
func (v uint64Order) Less(i, j int) bool { return v[i] < v[j] }
func (v uint64Order) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }

type float32Order []float32

func (v float32Order) Len() int           { return len(v) }
func (v float32Order) Less(i, j int) bool { return v[i] < v[j] }
func (v float32Order) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }

type float64Order []float64

func (v float64Order) Len() int           { return len(v) }
func (v float64Order) Less(i, j int) bool { return v[i] < v[j] }
func (v float64Order) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }

type bytesOrder [][]byte

func (v bytesOrder) Len() int           { return len(v) }
func (v bytesOrder) Less(i, j int) bool { return bytes.Compare(v[i], v[j]) < 0 }
func (v bytesOrder) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }

func orderingName(ordering int) string {
	switch {
	case isAscending(ordering):
		return "ascending"
	case isDescending(ordering):
		return "descending"
	default:
		return "undefined"
	}
}

func isAscending(ordering int) bool {
	return ordering > 0
}

func isDescending(ordering int) bool {
	return ordering < 0
}

func isUndefined(ordering int) bool {
	return ordering == 0
}

func isOrdered(set sort.Interface) bool {
	return set.Len() > 1 && sort.IsSorted(set)
}

func checkOrdering(t *testing.T, set sort.Interface, ordering int) bool {
	t.Helper()
	switch {
	case isOrdered(set):
		if !isAscending(ordering) {
			t.Errorf("got=%s want=ascending", orderingName(ordering))
			return false
		}
	case isOrdered(sort.Reverse(set)):
		if !isDescending(ordering) {
			t.Errorf("got=%s want=descending", orderingName(ordering))
			return false
		}
	default:
		if !isUndefined(ordering) {
			t.Errorf("got=%s want=undefined", orderingName(ordering))
			return false
		}
	}
	return true
}

func TestOrderOfBool(t *testing.T) {
	check := func(values []bool) bool {
		return checkOrdering(t, boolOrder(values), orderOfBool(values))
	}
	err := quick.Check(func(values []bool) bool {
		if !check(values) {
			return false
		}
		sort.Sort(boolOrder(values))
		if !check(values) {
			return false
		}
		sort.Sort(sort.Reverse(boolOrder(values)))
		if !check(values) {
			return false
		}
		return true
	})
	if err != nil {
		t.Error(err)
	}
}

func TestOrderOfInt32(t *testing.T) {
	check := func(values []int32) bool {
		return checkOrdering(t, int32Order(values), orderOfInt32(values))
	}
	err := quick.Check(func(values []int32) bool {
		if !check(values) {
			return false
		}
		sort.Sort(int32Order(values))
		if !check(values) {
			return false
		}
		sort.Sort(sort.Reverse(int32Order(values)))
		if !check(values) {
			return false
		}
		return true
	})
	if err != nil {
		t.Error(err)
	}

	// This extra test validates that out-of-order values at 64 byte boundaries
	// are properly detected; it tests corner cases of the vectorized code path
	// which works on 64 bytes per loop iteration.
	values := []int32{
		0, 1, 2, 3, 4, 5, 6, 7,
		8, 9, 10, 11, 12, 13, 14, 15,
		// 15 > 14, the algorithm must detect that the values are not ordered.
		14, 17, 18, 19, 20, 21, 22, 23,
		24, 25, 26, 27, 28, 29, 30, 31,
	}

	if !check(values) {
		t.Error("failed due to not checking the connection between sequences of 16 elements")
	}
}

func TestOrderOfInt64(t *testing.T) {
	check := func(values []int64) bool {
		return checkOrdering(t, int64Order(values), orderOfInt64(values))
	}
	err := quick.Check(func(values []int64) bool {
		if !check(values) {
			return false
		}
		sort.Sort(int64Order(values))
		if !check(values) {
			return false
		}
		sort.Sort(sort.Reverse(int64Order(values)))
		if !check(values) {
			return false
		}
		return true
	})
	if err != nil {
		t.Error(err)
	}

	values := []int64{
		0, 1, 2, 3, 4, 5, 6, 7,
		6, 9, 10, 11, 12, 13, 14, 15,
		14, 17, 18, 19, 20, 21, 22, 23,
		24, 25, 26, 27, 28, 29, 30, 31,
	}

	if !check(values) {
		t.Error("failed due to not checking the connection between sequences of 8 elements")
	}
}

func TestOrderOfUint32(t *testing.T) {
	check := func(values []uint32) bool {
		return checkOrdering(t, uint32Order(values), orderOfUint32(values))
	}
	err := quick.Check(func(values []uint32) bool {
		if !check(values) {
			return false
		}
		sort.Sort(uint32Order(values))
		if !check(values) {
			return false
		}
		sort.Sort(sort.Reverse(uint32Order(values)))
		if !check(values) {
			return false
		}
		return true
	})
	if err != nil {
		t.Error(err)
	}

	values := []uint32{
		0, 1, 2, 3, 4, 5, 6, 7,
		8, 9, 10, 11, 12, 13, 14, 15,
		14, 17, 18, 19, 20, 21, 22, 23,
		24, 25, 26, 27, 28, 29, 30, 31,
	}

	if !check(values) {
		t.Error("failed due to not checking the connection between sequences of 16 elements")
	}
}

func TestOrderOfUint64(t *testing.T) {
	check := func(values []uint64) bool {
		return checkOrdering(t, uint64Order(values), orderOfUint64(values))
	}
	err := quick.Check(func(values []uint64) bool {
		if !check(values) {
			return false
		}
		sort.Sort(uint64Order(values))
		if !check(values) {
			return false
		}
		sort.Sort(sort.Reverse(uint64Order(values)))
		if !check(values) {
			return false
		}
		return true
	})
	if err != nil {
		t.Error(err)
	}

	values := []uint64{
		0, 1, 2, 3, 4, 5, 6, 7,
		6, 9, 10, 11, 12, 13, 14, 15,
		14, 17, 18, 19, 20, 21, 22, 23,
		24, 25, 26, 27, 28, 29, 30, 31,
	}

	if !check(values) {
		t.Error("failed due to not checking the connection between sequences of 8 elements")
	}
}

func TestOrderOfFloat32(t *testing.T) {
	check := func(values []float32) bool {
		return checkOrdering(t, float32Order(values), orderOfFloat32(values))
	}
	err := quick.Check(func(values []float32) bool {
		if !check(values) {
			return false
		}
		sort.Sort(float32Order(values))
		if !check(values) {
			return false
		}
		sort.Sort(sort.Reverse(float32Order(values)))
		if !check(values) {
			return false
		}
		return true
	})
	if err != nil {
		t.Error(err)
	}

	values := []float32{
		0, 1, 2, 3, 4, 5, 6, 7,
		8, 9, 10, 11, 12, 13, 14, 15,
		14, 17, 18, 19, 20, 21, 22, 23,
		24, 25, 26, 27, 28, 29, 30, 31,
	}

	if !check(values) {
		t.Error("failed due to not checking the connection between sequences of 16 elements")
	}
}

func TestOrderOfFloat64(t *testing.T) {
	check := func(values []float64) bool {
		return checkOrdering(t, float64Order(values), orderOfFloat64(values))
	}
	err := quick.Check(func(values []float64) bool {
		if !check(values) {
			return false
		}
		sort.Sort(float64Order(values))
		if !check(values) {
			return false
		}
		sort.Sort(sort.Reverse(float64Order(values)))
		if !check(values) {
			return false
		}
		return true
	})
	if err != nil {
		t.Error(err)
	}

	values := []float64{
		0, 1, 2, 3, 4, 5, 6, 7,
		6, 9, 10, 11, 12, 13, 14, 15,
		14, 17, 18, 19, 20, 21, 22, 23,
		24, 25, 26, 27, 28, 29, 30, 31,
	}

	if !check(values) {
		t.Error("failed due to not checking the connection between sequences of 8 elements")
	}
}

func TestOrderOfBytes(t *testing.T) {
	check := func(values [][]byte) bool {
		return checkOrdering(t, bytesOrder(values), orderOfBytes(values))
	}
	err := quick.Check(func(values [][16]byte) bool {
		slices := make([][]byte, len(values))
		for i := range values {
			slices[i] = values[i][:]
		}
		if !check(slices) {
			return false
		}
		sort.Sort(bytesOrder(slices))
		if !check(slices) {
			return false
		}
		sort.Sort(sort.Reverse(bytesOrder(slices)))
		if !check(slices) {
			return false
		}
		return true
	})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkOrderOfBool(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]bool, bufferSize/1)
		for i := 0; i < b.N; i++ {
			orderOfBool(values)
		}
	})
}

func BenchmarkOrderOfInt32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int32, bufferSize/4)
		for i := 0; i < b.N; i++ {
			orderOfInt32(values)
		}
	})
}

func BenchmarkOrderOfInt64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int64, bufferSize/8)
		for i := 0; i < b.N; i++ {
			orderOfInt64(values)
		}
	})
}

func BenchmarkOrderOfUint32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint32, bufferSize/4)
		for i := 0; i < b.N; i++ {
			orderOfUint32(values)
		}
	})
}

func BenchmarkOrderOfUint64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint64, bufferSize/8)
		for i := 0; i < b.N; i++ {
			orderOfUint64(values)
		}
	})
}

func BenchmarkOrderOfFloat32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float32, bufferSize/4)
		for i := 0; i < b.N; i++ {
			orderOfFloat32(values)
		}
	})
}

func BenchmarkOrderOfFloat64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float64, bufferSize/8)
		for i := 0; i < b.N; i++ {
			orderOfFloat64(values)
		}
	})
}

func BenchmarkOrderOfBytes(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		data := make([]byte, bufferSize)
		values := make([][]byte, len(data)/16)
		for i := range values {
			values[i] = data[i*16 : (i+1)*16]
		}
		for i := 0; i < b.N; i++ {
			orderOfBytes(values)
		}
	})
}
