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

func TestBlockDeltaInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockDeltaInt32(t, blockDeltaInt32AVX2)
}

func TestBlockMinInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockMinInt32(t, blockMinInt32AVX2)
}

func TestBlockSubInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockSubInt32(t, blockSubInt32AVX2)
}

func TestBlockBitWidthsInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockBitWidthsInt32(t, blockBitWidthsInt32AVX2)
}

func TestMiniBlockPackInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testMiniBlockPackInt32(t,
		func(dst []byte, src *[miniBlockSize]int32, bitWidth uint) {
			miniBlockPackInt32AVX2(&dst[0], src, bitWidth)
		},
	)
}

func BenchmarkBlockDeltaInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockDeltaInt32(b, blockDeltaInt32AVX2)
}

func BenchmarkBlockMinInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockMinInt32(b, blockMinInt32AVX2)
}

func BenchmarkBlockSubInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockSubInt32(b, blockSubInt32AVX2)
}

func BenchmarkBlockBitWidthsInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockBitWidthsInt32(b, blockBitWidthsInt32AVX2)
}

func BenchmarkMiniBlockPackInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkMiniBlockPackInt32(b,
		func(dst []byte, src *[miniBlockSize]int32, bitWidth uint) {
			miniBlockPackInt32AVX2(&dst[0], src, bitWidth)
		},
	)
}
