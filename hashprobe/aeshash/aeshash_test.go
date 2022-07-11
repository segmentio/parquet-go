package aeshash

import (
	"encoding/binary"
	"testing"
	"time"
	"unsafe"
)

//go:noescape
//go:linkname runtime_memhash32 runtime.memhash32
func runtime_memhash32(data unsafe.Pointer, seed uintptr) uintptr

//go:noescape
//go:linkname runtime_memhash64 runtime.memhash64
func runtime_memhash64(data unsafe.Pointer, seed uintptr) uintptr

//go:noescape
//go:linkname runtime_memhash runtime.memhash
func runtime_memhash(data unsafe.Pointer, seed, size uintptr) uintptr

func memhash32(data uint32, seed uintptr) uintptr {
	return runtime_memhash32(unsafe.Pointer(&data), seed)
}

func memhash64(data uint64, seed uintptr) uintptr {
	return runtime_memhash64(unsafe.Pointer(&data), seed)
}

func memhash128(data [16]byte, seed uintptr) uintptr {
	return runtime_memhash(unsafe.Pointer(&data), seed, 16)
}

func TestHash32(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

	h0 := memhash32(42, 1)
	h1 := Hash32(42, 1)

	if h0 != h1 {
		t.Errorf("want=%016x got=%016x", h0, h1)
	}
}

func TestMultiHash32(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

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
			t.Errorf("hash(%d): want=%016x got=%016x", values[i], h, hashes[i])
		}
	}
}

func TestHash64(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

	h0 := memhash64(42, 1)
	h1 := Hash64(42, 1)

	if h0 != h1 {
		t.Errorf("want=%016x got=%016x", h0, h1)
	}
}

func TestMultiHash64(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

	const N = 10
	hashes := [N]uintptr{}
	values := [N]uint64{}
	seed := uintptr(64)

	for i := range values {
		values[i] = uint64(i)
	}

	MultiHash64(hashes[:], values[:], seed)

	for i := range values {
		h := Hash64(values[i], seed)

		if h != hashes[i] {
			t.Errorf("hash(%d): want=%016x got=%016x", values[i], h, hashes[i])
		}
	}
}

func BenchmarkMultiHash64(b *testing.B) {
	if !Enabled() {
		b.Skip("AES hash not supported on this platform")
	}

	hashes := [512]uintptr{}
	values := [512]uint64{}
	b.SetBytes(8 * int64(len(hashes)))
	benchmarkHashThroughput(b, func(seed uintptr) int {
		MultiHash64(hashes[:], values[:], seed)
		return len(hashes)
	})
}

func TestHash128(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

	h0 := memhash128([16]byte{0: 42}, 1)
	h1 := Hash128([16]byte{0: 42}, 1)

	if h0 != h1 {
		t.Errorf("want=%016x got=%016x", h0, h1)
	}
}

func TestMultiHash128(t *testing.T) {
	if !Enabled() {
		t.Skip("AES hash not supported on this platform")
	}

	const N = 10
	hashes := [N]uintptr{}
	values := [N][16]byte{}
	seed := uintptr(128)

	for i := range values {
		binary.LittleEndian.PutUint64(values[i][:8], uint64(i))
	}

	MultiHash128(hashes[:], values[:], seed)

	for i := range values {
		h := Hash128(values[i], seed)

		if h != hashes[i] {
			t.Errorf("hash(%d): want=%016x got=%016x", values[i], h, hashes[i])
		}
	}
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
