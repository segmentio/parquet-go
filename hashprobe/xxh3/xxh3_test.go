package xxh3

import (
	"math/rand"
	"testing"
	"time"
)

func TestHash32(t *testing.T) {
	if h := Hash32(42, 0); h != 0x2132b64814a1ad5d {
		t.Errorf("hash mismatch: %08x", h)
	}
}

func TestMultiHash32(t *testing.T) {
	const N = 10
	hashes := [N]uintptr{}
	values := [N]uint32{}
	seed := uintptr(32)

	for i := range values {
		values[i] = uint32(i)
	}

	MultiHash32(hashes[:], values[:], seed)

	for i := range values {
		h := Hash32(values[i], seed)

		if h != hashes[i] {
			t.Errorf("hash(%d): want=%08x got=%08x", values[i], h, hashes[i])
		}
	}
}

func BenchmarkHash32(b *testing.B) {
	b.SetBytes(8)
	value := rand.Uint32()
	benchmarkHashThroughput(b, func(seed uintptr) int {
		value = uint32(Hash32(value, seed))
		return 1
	})
}

func BenchmarkMultiHash32(b *testing.B) {
	hashes := [512]uintptr{}
	values := [512]uint32{}
	b.SetBytes(4 * int64(len(hashes)))
	benchmarkHashThroughput(b, func(seed uintptr) int {
		MultiHash32(hashes[:], values[:], seed)
		return len(hashes)
	})
}

func benchmarkHashThroughput(b *testing.B, f func(seed uintptr) int) {
	hashes := int64(0)
	start := time.Now()

	for i := 0; i < b.N; i++ {
		hashes += int64(f(uintptr(i)))
	}

	seconds := time.Since(start).Seconds()
	b.ReportMetric(float64(hashes)/seconds, "hash/s")
}
