//go:build amd64 && !purego

package delta

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/segmentio/parquet-go/internal/bits"
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

func TestBlockMinInt32(t *testing.T) {
	testBlockMinInt32(t, blockMinInt32)
}

func TestBlockMinInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockMinInt32(t, blockMinInt32AVX2)
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

func TestBlockSubInt32(t *testing.T) {
	testBlockSubInt32(t, blockSubInt32)
}

func TestBlockSubInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockSubInt32(t, blockSubInt32AVX2)
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

func TestBlockBitWidthsInt32(t *testing.T) {
	testBlockBitWidthsInt32(t, blockBitWidthsInt32)
}

func TestBlockBitWidthsInt32AVX2(t *testing.T) {
	requireAVX2(t)
	testBlockBitWidthsInt32(t, blockBitWidthsInt32AVX2)
}

func testBlockBitWidthsInt32(t *testing.T, f func(*[numMiniBlocks]byte, *[blockSize]int32)) {
	bitWidths := [numMiniBlocks]byte{}
	block := [blockSize]int32{}
	for i := range block {
		block[i] = int32(i)
	}
	f(&bitWidths, &block)

	want := [numMiniBlocks]byte{}
	for i := range want {
		j := (i + 0) * miniBlockSize
		k := (i + 1) * miniBlockSize
		want[i] = byte(bits.MaxLen32(block[j:k]))
	}

	if bitWidths != want {
		t.Errorf("wrong bit widths: want=%d got=%d", want, bitWidths)
	}
}

func TestMiniBlockPackInt32(t *testing.T) {
	testMiniBlockPackInt32(t, miniBlockPackInt32Default)
}

func TestMiniBlockPackInt32AVX2(t *testing.T) {
	testMiniBlockPackInt32(t, miniBlockPackInt32AVX2)
}

func testMiniBlockPackInt32(t *testing.T, f func(*byte, *[miniBlockSize]int32, uint)) {
	for bitWidth := uint(1); bitWidth <= 32; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(t *testing.T) {
			got := [4 * miniBlockSize]byte{}
			src := [miniBlockSize]int32{}
			for i := range src {
				src[i] = int32(i) & int32((1<<bitWidth)-1)
			}

			want := [4 * miniBlockSize]byte{}
			bitOffset := uint(0)

			for _, bits := range src {
				for b := uint(0); b < bitWidth; b++ {
					x := bitOffset / 8
					y := bitOffset % 8
					want[x] |= byte(((bits >> b) & 1) << y)
					bitOffset++
				}
			}

			f(&got[0], &src, bitWidth)
			n := (miniBlockSize * bitWidth) / 8

			if !bytes.Equal(want[:n], got[:n]) {
				t.Errorf("output mismatch: want=%08x got=%08x", want[:n], got[:n])
			}
		})
	}
}

func BenchmarkBlockDeltaInt32(b *testing.B) {
	benchmarkBlockDeltaInt32(b, blockDeltaInt32)
}

func BenchmarkBlockDeltaInt32AVX2(b *testing.B) {
	benchmarkBlockDeltaInt32(b, blockDeltaInt32AVX2)
}

func benchmarkBlockDeltaInt32(b *testing.B, f func(*[blockSize]int32, int32) int32) {
	b.SetBytes(4 * blockSize)
	block := [blockSize]int32{}
	for i := 0; i < b.N; i++ {
		_ = f(&block, 0)
	}
}

func BenchmarkBlockMinInt32(b *testing.B) {
	benchmarkBlockMinInt32(b, blockMinInt32)
}

func BenchmarkBlockMinInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockMinInt32(b, blockMinInt32AVX2)
}

func benchmarkBlockMinInt32(b *testing.B, f func(*[blockSize]int32) int32) {
	b.SetBytes(4 * blockSize)
	block := [blockSize]int32{}
	for i := 0; i < b.N; i++ {
		_ = f(&block)
	}
}

func BenchmarkBlockSubInt32(b *testing.B) {
	benchmarkBlockSubInt32(b, blockSubInt32)
}

func BenchmarkBlockSubInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockSubInt32(b, blockSubInt32AVX2)
}

func benchmarkBlockSubInt32(b *testing.B, f func(*[blockSize]int32, int32)) {
	b.SetBytes(4 * blockSize)
	block := [blockSize]int32{}
	for i := 0; i < b.N; i++ {
		f(&block, 42)
	}
}

func BenchmarkBlockBitWidthsInt32(b *testing.B) {
	benchmarkBlockBitWidthsInt32(b, blockBitWidthsInt32)
}

func BenchmarkBlockBitWidthsInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkBlockBitWidthsInt32(b, blockBitWidthsInt32AVX2)
}

func benchmarkBlockBitWidthsInt32(b *testing.B, f func(*[numMiniBlocks]byte, *[blockSize]int32)) {
	b.SetBytes(4 * blockSize)
	bitWidths := [numMiniBlocks]byte{}
	block := [blockSize]int32{}
	for i := 0; i < b.N; i++ {
		f(&bitWidths, &block)
	}
}

func BenchmarkMiniBlockPackInt32(b *testing.B) {
	benchmarkMiniBlockPackInt32(b, miniBlockPackInt32Default)
}

func BenchmarkMiniBlockPackInt32AVX2(b *testing.B) {
	requireAVX2(b)
	benchmarkMiniBlockPackInt32(b, miniBlockPackInt32AVX2)
}

func benchmarkMiniBlockPackInt32(b *testing.B, f func(*byte, *[miniBlockSize]int32, uint)) {
	for bitWidth := uint(1); bitWidth <= 32; bitWidth++ {
		b.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(b *testing.B) {
			b.SetBytes(4 * miniBlockSize)
			dst := [4 * miniBlockSize]byte{}
			src := [miniBlockSize]int32{}
			for i := 0; i < b.N; i++ {
				f(&dst[0], &src, bitWidth)
			}
		})
	}
}
