package bits_test

import (
	"sort"
	"testing"

	"github.com/segmentio/parquet-go/internal/bits"
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

const (
	ascending  = "ascending"
	descending = "descending"
	undefined  = "undefined"
)

func orderingName(ordering int) string {
	switch {
	case isAscending(ordering):
		return ascending
	case isDescending(ordering):
		return descending
	default:
		return undefined
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

func isSorted(set sort.Interface) bool {
	return set.Len() > 0 && sort.IsSorted(set)
}

func checkOrdering(t *testing.T, set sort.Interface, ordering int) bool {
	t.Helper()
	switch {
	case isSorted(set):
		if !isAscending(ordering) {
			t.Errorf("got=%s want=%s", orderingName(ordering), ascending)
			return false
		}
	case isSorted(sort.Reverse(set)):
		if !isDescending(ordering) {
			t.Errorf("got=%s want=%s", orderingName(ordering), descending)
			return false
		}
	default:
		if !isUndefined(ordering) {
			t.Errorf("got=%s want=%s", orderingName(ordering), undefined)
			return false
		}
	}
	return true
}

func TestOrderOfBool(t *testing.T) {
	check := func(values []bool) bool {
		return checkOrdering(t, boolOrder(values), bits.OrderOfBool(values))
	}
	err := quickCheck(func(values []bool) bool {
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
		return checkOrdering(t, int32Order(values), bits.OrderOfInt32(values))
	}
	err := quickCheck(func(values []int32) bool {
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
}

func TestOrderOfInt64(t *testing.T) {
	check := func(values []int64) bool {
		return checkOrdering(t, int64Order(values), bits.OrderOfInt64(values))
	}
	err := quickCheck(func(values []int64) bool {
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
}

func TestOrderOfUint32(t *testing.T) {
	check := func(values []uint32) bool {
		return checkOrdering(t, uint32Order(values), bits.OrderOfUint32(values))
	}
	err := quickCheck(func(values []uint32) bool {
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
}

func TestOrderOfUint64(t *testing.T) {
	check := func(values []uint64) bool {
		return checkOrdering(t, uint64Order(values), bits.OrderOfUint64(values))
	}
	err := quickCheck(func(values []uint64) bool {
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
}

func TestOrderOfFloat32(t *testing.T) {
	check := func(values []float32) bool {
		return checkOrdering(t, float32Order(values), bits.OrderOfFloat32(values))
	}
	err := quickCheck(func(values []float32) bool {
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
}

func TestOrderOfFloat64(t *testing.T) {
	check := func(values []float64) bool {
		return checkOrdering(t, float64Order(values), bits.OrderOfFloat64(values))
	}
	err := quickCheck(func(values []float64) bool {
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
}

func BenchmarkOrderOfBool(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]bool, bufferSize/1)
		for i := 0; i < b.N; i++ {
			bits.OrderOfBool(values)
		}
	})
}

func BenchmarkOrderOfInt32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int32, bufferSize/1)
		for i := 0; i < b.N; i++ {
			bits.OrderOfInt32(values)
		}
	})
}

func BenchmarkOrderOfInt64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]int64, bufferSize/1)
		for i := 0; i < b.N; i++ {
			bits.OrderOfInt64(values)
		}
	})
}

func BenchmarkOrderOfUint32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint32, bufferSize/1)
		for i := 0; i < b.N; i++ {
			bits.OrderOfUint32(values)
		}
	})
}

func BenchmarkOrderOfUint64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]uint64, bufferSize/1)
		for i := 0; i < b.N; i++ {
			bits.OrderOfUint64(values)
		}
	})
}

func BenchmarkOrderOfFloat32(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float32, bufferSize/1)
		for i := 0; i < b.N; i++ {
			bits.OrderOfFloat32(values)
		}
	})
}

func BenchmarkOrderOfFloat64(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B, bufferSize int) {
		values := make([]float64, bufferSize/1)
		for i := 0; i < b.N; i++ {
			bits.OrderOfFloat64(values)
		}
	})
}
