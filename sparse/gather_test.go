package sparse_test

import (
	"encoding/binary"
	"math"
	"testing"
	"time"
	"unsafe"

	"github.com/segmentio/parquet-go/sparse"
)

const (
	benchmarkGatherPerLoop = 1000
)

func TestGatherUint32(t *testing.T) {
	type point2D struct{ X, Y uint32 }

	const N = 100
	buf := make([]point2D, N+1)
	dst := make([]uint32, N)
	src := sparse.UnsafeUint32Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	for i := range buf {
		buf[i].X = math.MaxUint32
		buf[i].Y = uint32(2 * i)
	}

	if n := sparse.GatherUint32(dst, src); n != N {
		t.Errorf("wrong number of values gathered: want=%d got=%d", N, n)
	}

	for i, v := range dst {
		if v != uint32(2*i) {
			t.Errorf("wrong value gathered at index %d: want=%d got=%d", i, 2*i, v)
		}
	}
}

func TestGatherUint64(t *testing.T) {
	type point2D struct{ X, Y uint64 }

	const N = 100
	buf := make([]point2D, N+1)
	dst := make([]uint64, N)
	src := sparse.UnsafeUint64Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	for i := range buf {
		buf[i].X = math.MaxUint64
		buf[i].Y = uint64(2 * i)
	}

	if n := sparse.GatherUint64(dst, src); n != N {
		t.Errorf("wrong number of values gathered: want=%d got=%d", N, n)
	}

	for i, v := range dst {
		if v != uint64(2*i) {
			t.Errorf("wrong value gathered at index %d: want=%d got=%d", i, 2*i, v)
		}
	}
}

func TestGatherUint128(t *testing.T) {
	type point2D struct{ X, Y [16]byte }

	const N = 100
	buf := make([]point2D, N+1)
	dst := make([][16]byte, N)
	src := sparse.UnsafeUint128Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	for i := range buf {
		x := uint64(math.MaxUint64)
		y := uint64(2 * i)
		binary.LittleEndian.PutUint64(buf[i].X[:], x)
		binary.LittleEndian.PutUint64(buf[i].Y[:], y)
	}

	if n := sparse.GatherUint128(dst, src); n != N {
		t.Errorf("wrong number of values gathered: want=%d got=%d", N, n)
	}

	for i, v := range dst {
		if y := binary.LittleEndian.Uint64(v[:]); y != uint64(2*i) {
			t.Errorf("wrong value gathered at index %d: want=%d got=%d", i, 2*i, y)
		}
	}
}

func BenchmarkGather32(b *testing.B) {
	type point2D struct{ X, Y uint32 }

	buf := make([]point2D, benchmarkGatherPerLoop)
	dst := make([]uint32, benchmarkGatherPerLoop)
	src := sparse.UnsafeUint32Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	b.SetBytes(4 * benchmarkGatherPerLoop)
	benchmarkThroughput(b, "gather", func() int {
		return sparse.GatherUint32(dst, src)
	})
}

func BenchmarkGather64(b *testing.B) {
	type point2D struct{ X, Y uint64 }

	buf := make([]point2D, benchmarkGatherPerLoop)
	dst := make([]uint64, benchmarkGatherPerLoop)
	src := sparse.UnsafeUint64Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	b.SetBytes(8 * benchmarkGatherPerLoop)
	benchmarkThroughput(b, "gather", func() int {
		return sparse.GatherUint64(dst, src)
	})
}

func BenchmarkGather128(b *testing.B) {
	type point2D struct{ X, Y [16]byte }

	buf := make([]point2D, benchmarkGatherPerLoop)
	dst := make([][16]byte, benchmarkGatherPerLoop)
	src := sparse.UnsafeUint128Array(unsafe.Pointer(&buf[0].Y), len(buf), unsafe.Sizeof(buf[0]))

	b.SetBytes(16 * benchmarkGatherPerLoop)
	benchmarkThroughput(b, "gather", func() int {
		return sparse.GatherUint128(dst, src)
	})
}

func benchmarkThroughput(b *testing.B, m string, f func() int) {
	start := time.Now()
	count := 0

	for i := 0; i < b.N; i++ {
		count += f()
	}

	seconds := time.Since(start).Seconds()
	b.ReportMetric(float64(count)/seconds, m+"/s")
}