//go:build !purego

package delta

import (
	"golang.org/x/sys/cpu"
)

func init() {
	if cpu.X86.HasAVX2 {
		encodeInt32 = encodeInt32AVX2
	}
}

//go:noescape
func blockDeltaInt32AVX2(block *[blockSize]int32, lastValue int32) int32

//go:noescape
func blockMinInt32AVX2(block *[blockSize]int32) int32

//go:noescape
func blockSubInt32AVX2(block *[blockSize]int32, value int32)

//go:noescape
func blockBitWidthsInt32AVX2(bitWidths *[numMiniBlocks]byte, block *[blockSize]int32)

//go:noescape
func miniBlockPackInt32Default(dst *byte, src *[miniBlockSize]int32, bitWidth uint)

//go:noescape
func miniBlockPackInt32x1bitAVX2(dst *byte, src *[miniBlockSize]int32)

//go:noescape
func miniBlockPackInt32x2bitsAVX2(dst *byte, src *[miniBlockSize]int32)

//go:noescape
func miniBlockPackInt32x3to16bitsAVX2(dst *byte, src *[miniBlockSize]int32, bitWidth uint)

//go:noescape
func miniBlockPackInt32x32bitsAVX2(dst *byte, src *[miniBlockSize]int32)

func miniBlockPackInt32(dst []byte, src *[miniBlockSize]int32, bitWidth uint) {
	miniBlockPackInt32Default(&dst[0], src, bitWidth)
}

func miniBlockPackInt32AVX2(dst *byte, src *[miniBlockSize]int32, bitWidth uint) {
	switch {
	case bitWidth == 1:
		miniBlockPackInt32x1bitAVX2(dst, src)
	case bitWidth == 2:
		miniBlockPackInt32x2bitsAVX2(dst, src)
	case bitWidth == 32:
		miniBlockPackInt32x32bitsAVX2(dst, src)
	case bitWidth <= 16:
		miniBlockPackInt32x3to16bitsAVX2(dst, src, bitWidth)
	default:
		miniBlockPackInt32Default(dst, src, bitWidth)
	}
}

func encodeInt32AVX2(dst []byte, src []int32) []byte {
	totalValues := len(src)
	firstValue := int32(0)
	if totalValues > 0 {
		firstValue = src[0]
	}

	n := len(dst)
	dst = resize(dst, n+maxHeaderLength32)
	dst = dst[:n+encodeBinaryPackedHeader(dst[n:], blockSize, numMiniBlocks, totalValues, int64(firstValue))]

	if totalValues < 2 {
		return dst
	}

	lastValue := firstValue
	for i := 1; i < len(src); i += blockSize {
		block := [blockSize]int32{}
		blockLength := copy(block[:], src[i:])

		lastValue = blockDeltaInt32AVX2(&block, lastValue)
		minDelta := blockMinInt32AVX2(&block)
		blockSubInt32AVX2(&block, minDelta)
		blockClearInt32(&block, blockLength)

		bitWidths := [numMiniBlocks]byte{}
		blockBitWidthsInt32AVX2(&bitWidths, &block)

		n := len(dst)
		dst = resize(dst, n+maxMiniBlockLength32+16)
		n += encodeBlockHeader(dst[n:], int64(minDelta), bitWidths)

		for i, bitWidth := range bitWidths {
			if bitWidth != 0 {
				miniBlock := (*[miniBlockSize]int32)(block[i*miniBlockSize:])
				miniBlockPackInt32AVX2(&dst[n], miniBlock, uint(bitWidth))
				n += (miniBlockSize * int(bitWidth)) / 8
			}
		}

		dst = dst[:n]
	}

	return dst
}
