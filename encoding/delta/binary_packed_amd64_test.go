//go:build amd64 && !purego

package delta

import (
	"testing"

	"golang.org/x/sys/cpu"
)

func requireAVX2(t testing.TB) {
	if !cpu.X86.HasAVX2 {
		t.Skip("CPU does not support AVX2")
	}
}

func TestBlockDeltaInt32(t *testing.T) {
	testBlockDeltaInt32(t, blockDeltaInt32)
}

func TestBlockDeltaInt32AVX2(t *testing.T) {
	testBlockDeltaInt32(t, blockDeltaInt32AVX2)
}

func TestBlockMinInt32(t *testing.T) {
	testBlockMinInt32(t, blockMinInt32)
}

func TestBlockMinInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockMinInt32(t, blockMinInt32AVX2)
}

func TestBlockSubInt32(t *testing.T) {
	testBlockSubInt32(t, blockSubInt32)
}

func TestBlockSubInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockSubInt32(t, blockSubInt32AVX2)
}

func testBlockDeltaInt32(t *testing.T, f func(*[blockSize]int32, int32) int32) {
	block := [blockSize]int32{}
	for i := range block {
		block[i] = int32(2 * (i + 1))
	}
	lastValue := f(&block, 0)
	if lastValue != 2*blockSize {
		t.Errorf("wrong last block value: want=%d got=%d", 2*blockSize, lastValue)
	}
	for i := range block {
		j := int32(2 * (i + 0))
		k := int32(2 * (i + 1))
		if block[i] != (k - j) {
			t.Errorf("wrong block delta at index %d: want=%d got=%d", i, k-j, block[i])
		}
	}
}

func testBlockMinInt32(t *testing.T, f func(*[blockSize]int32) int32) {
	block := [blockSize]int32{}
	for i := range block {
		block[i] = blockSize - int32(i)
	}
	if min := f(&block); min != 1 {
		t.Errorf("wrong min block value: want=1 got=%d", min)
	}
}

func testBlockSubInt32(t *testing.T, f func(*[blockSize]int32, int32)) {
	block := [blockSize]int32{}
	for i := range block {
		block[i] = int32(i)
	}
	f(&block, 1)
	for i := range block {
		if block[i] != int32(i-1) {
			t.Errorf("wrong block value at index %d: want=%d got=%d", i, i-1, block[i])
		}
	}
}

func BenchmarkBlockDeltaInt32(b *testing.B) {
	benchmarkBlockDeltaInt32(b, blockDeltaInt32)
}

func BenchmarkBlockDeltaInt32AVX2(b *testing.B) {
	benchmarkBlockDeltaInt32(b, blockDeltaInt32AVX2)
}

func BenchmarkBlockMinInt32(b *testing.B) {
	benchmarkBlockMinInt32(b, blockMinInt32)
}

func BenchmarkBlockMinInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockMinInt32(b, blockMinInt32AVX2)
}

func BenchmarkBlockSubInt32(b *testing.B) {
	benchmarkBlockSubInt32(b, blockSubInt32)
}

func BenchmarkBlockSubInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockSubInt32(b, blockSubInt32AVX2)
}

func benchmarkBlockDeltaInt32(b *testing.B, f func(*[blockSize]int32, int32) int32) {
	b.SetBytes(4 * blockSize)
	block := [blockSize]int32{}
	for i := 0; i < b.N; i++ {
		_ = f(&block, 0)
	}
}

func benchmarkBlockMinInt32(b *testing.B, f func(*[blockSize]int32) int32) {
	b.SetBytes(4 * blockSize)
	block := [blockSize]int32{}
	for i := 0; i < b.N; i++ {
		_ = f(&block)
	}
}

func benchmarkBlockSubInt32(b *testing.B, f func(*[blockSize]int32, int32)) {
	b.SetBytes(4 * blockSize)
	block := [blockSize]int32{}
	for i := 0; i < b.N; i++ {
		f(&block, 42)
	}
}
