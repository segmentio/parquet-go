package hashprobe

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestUint64TableProbeOneByOne(t *testing.T) {
	const N = 500
	table := NewUint64Table(0, 0.9)

	for n := 0; n < 2; n++ {
		// Do two passes, all should behave the same.
		for i := 1; i <= N; i++ {
			k := [1]uint64{}
			v := [1]int32{}

			k[0] = uint64(i)
			table.Probe(k[:], v[:])

			if v[0] != int32(i-1) {
				t.Errorf("wrong value probed for key=%d: want=%d got=%d", i, i-1, v[0])
			}
		}
	}
}

type uint64Table interface {
	Reset()
	Len() int
	Probe([]uint64, []int32)
}

type uint64Map map[uint64]int32

func (m uint64Map) Reset() {
	for k := range m {
		delete(m, k)
	}
}

func (m uint64Map) Len() int {
	return len(m)
}

func (m uint64Map) Probe(keys []uint64, values []int32) {
	_ = values[:len(keys)]

	for i, k := range keys {
		v, ok := m[k]
		if !ok {
			v = int32(len(m))
			m[k] = v
		}
		values[i] = v
	}
}

func BenchmarkUint64Table(b *testing.B) {
	benchmarkUint64Table(b, func(size int) uint64Table { return NewUint64Table(size, 0.9) })
}

func BenchmarkGoUint64Map(b *testing.B) {
	benchmarkUint64Table(b, func(size int) uint64Table { return make(uint64Map, size) })
}

func benchmarkUint64Table(b *testing.B, newTable func(size int) uint64Table) {
	for n := 100; n <= 1e6; n *= 10 {
		table := newTable(0)
		keys, values := generateUint64Table(n)

		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			benchmarkUint64Loop(b, table.Probe, keys, values)
		})
	}
}

func benchmarkUint64Loop(b *testing.B, f func([]uint64, []int32), keys []uint64, values []int32) {
	const N = 100
	i := 0
	j := N
	b.SetBytes(8 * N)

	_ = keys[:len(values)]
	_ = values[:len(keys)]
	start := time.Now()

	for k := 0; k < b.N; k++ {
		if j > len(keys) {
			j = len(keys)
		}
		f(keys[i:j:j], values[i:j:j])
		if j == len(keys) {
			i, j = 0, N
		} else {
			i, j = j, j+N
		}
	}

	seconds := time.Since(start).Seconds()
	b.ReportMetric(float64(N*b.N)/seconds, "probe/s")
}

func generateUint64Table(n int) ([]uint64, []int32) {
	prng := rand.New(rand.NewSource(int64(n)))
	keys := make([]uint64, n)
	values := make([]int32, n)

	for i := range keys {
		keys[i] = prng.Uint64()
	}

	return keys, values
}
