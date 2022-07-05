package wyhash

import (
	"math/rand"
	"testing"
	"time"
)

func TestSum32Uint32(t *testing.T) {
	if h := Sum32Uint32(42, 1); h != 0xe6b5a25e {
		t.Errorf("hash mismatch: %08x", h)
	}
}

func TestMultiSum32Uint32(t *testing.T) {
	const N = 10
	hashes := [N]uint32{}
	values := [N]uint32{}
	seed := uint32(32)

	for i := range values {
		values[i] = uint32(i)
	}

	MultiSum32Uint32(hashes[:], values[:], seed)

	for i := range values {
		h := Sum32Uint32(values[i], seed)

		if h != hashes[i] {
			t.Errorf("hash(%d): want=%08x got=%08x", values[i], h, hashes[i])
		}
	}
}

func TestSum64Uint64(t *testing.T) {
	if h := Sum64Uint64(42, 1); h != 0x6e69a6ede6b5a25e {
		t.Errorf("hash mismatch: %016x", h)
	}
}

func TestMultiSum64Uint64(t *testing.T) {
	const N = 10
	hashes := [N]uint64{}
	values := [N]uint64{}
	seed := uint64(64)

	for i := range values {
		values[i] = uint64(i)
	}

	MultiSum64Uint64(hashes[:], values[:], seed)

	for i := range values {
		h := Sum64Uint64(values[i], seed)

		if h != hashes[i] {
			t.Errorf("hash(%d): want=%016x got=%016x", values[i], h, hashes[i])
		}
	}
}

func BenchmarkSum64Uint64(b *testing.B) {
	b.SetBytes(8)
	value := rand.Uint64()
	benchmarkHashThroughput(b, func(seed uint64) int {
		value = Sum64Uint64(value, seed)
		return 1
	})
}

func BenchmarkMultiSum64Uint64(b *testing.B) {
	hashes := [512]uint64{}
	values := [512]uint64{}
	b.SetBytes(8 * int64(len(hashes)))
	benchmarkHashThroughput(b, func(seed uint64) int {
		MultiSum64Uint64(hashes[:], values[:], seed)
		return len(hashes)
	})
}

func benchmarkHashThroughput(b *testing.B, f func(uint64) int) {
	hashes := int64(0)
	start := time.Now()

	for i := 0; i < b.N; i++ {
		hashes += int64(f(uint64(i)))
	}

	seconds := time.Since(start).Seconds()
	b.ReportMetric(float64(hashes)/seconds, "hash/s")
}
