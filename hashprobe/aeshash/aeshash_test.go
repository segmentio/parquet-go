package aeshash

import (
	"testing"
	"time"
	"unsafe"
)

//go:linkname runtime_memhash64 runtime.memhash64
func runtime_memhash64(data unsafe.Pointer, seed uintptr) uintptr

func memhash64(data, seed uint64) uint64 {
	return uint64(runtime_memhash64(unsafe.Pointer(&data), uintptr(seed)))
}

func TestSum64Uint64(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

	h0 := memhash64(42, 1)
	h1 := Sum64Uint64(42, 1)

	if h0 != h1 {
		t.Errorf("want=%016x got=%016x", h0, h1)
	}
}

func TestMultiSum64Uint64(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

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

func BenchmarkMultiSum64Uint64(b *testing.B) {
	if !Enabled() {
		b.Skip("AES hash not supported on this platform")
	}

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
