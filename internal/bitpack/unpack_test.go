package bitpack_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/segmentio/parquet-go/internal/bitpack"
)

const (
	blockSize = 128
)

func TestUnpackInt32(t *testing.T) {
	for bitWidth := uint(1); bitWidth <= 32; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(t *testing.T) {
			block := [blockSize]int32{}
			bitMask := int32(bitWidth<<1) - 1

			prng := rand.New(rand.NewSource(0))
			for i := range block {
				block[i] = prng.Int31() & bitMask
			}

			size := (blockSize * bitWidth) / 8
			buf := make([]byte, size+bitpack.PaddingInt32)
			bitpack.PackInt32(buf, block[:], bitWidth)

			src := buf[:size]
			dst := make([]int32, blockSize)

			for n := 1; n <= blockSize; n++ {
				for i := range dst {
					dst[i] = 0
				}

				bitpack.UnpackInt32(dst[:n], src, bitWidth)

				if !reflect.DeepEqual(block[:n], dst[:n]) {
					t.Fatalf("values mismatch for length=%d\nwant: %v\ngot:  %v", n, block[:n], dst[:n])
				}
			}
		})
	}
}

func TestUnpackInt64(t *testing.T) {
	for bitWidth := uint(1); bitWidth <= 63; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(t *testing.T) {
			block := [blockSize]int64{}
			bitMask := int64(bitWidth<<1) - 1

			prng := rand.New(rand.NewSource(0))
			for i := range block {
				block[i] = prng.Int63() & bitMask
			}

			size := (blockSize * bitWidth) / 8
			buf := make([]byte, size+bitpack.PaddingInt64)
			bitpack.PackInt64(buf, block[:], bitWidth)

			src := buf[:size]
			dst := make([]int64, blockSize)

			for n := 1; n <= blockSize; n++ {
				for i := range dst {
					dst[i] = 0
				}

				bitpack.UnpackInt64(dst[:n], src, bitWidth)

				if !reflect.DeepEqual(block[:n], dst[:n]) {
					t.Fatalf("values mismatch for length=%d\nwant: %v\ngot:  %v", n, block[:n], dst[:n])
				}
			}
		})
	}
}

func BenchmarkUnpackInt32(b *testing.B) {
	for bitWidth := uint(1); bitWidth <= 32; bitWidth++ {
		block := [blockSize]int32{}
		buf := [4*blockSize + bitpack.PaddingInt32]byte{}
		bitpack.PackInt32(buf[:], block[:], bitWidth)

		b.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(b *testing.B) {
			dst := block[:]
			src := buf[:]

			for i := 0; i < b.N; i++ {
				bitpack.UnpackInt32(dst, src, bitWidth)
			}

			b.SetBytes(4 * blockSize)
		})
	}
}

func BenchmarkUnpackInt64(b *testing.B) {
	for bitWidth := uint(1); bitWidth <= 64; bitWidth++ {
		block := [blockSize]int64{}
		buf := [8*blockSize + bitpack.PaddingInt64]byte{}
		bitpack.PackInt64(buf[:], block[:], bitWidth)

		b.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(b *testing.B) {
			dst := block[:]
			src := buf[:]

			for i := 0; i < b.N; i++ {
				bitpack.UnpackInt64(dst, src, bitWidth)
			}

			b.SetBytes(4 * blockSize)
		})
	}
}
