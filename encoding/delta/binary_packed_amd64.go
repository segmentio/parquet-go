//go:build !purego

package delta

import (
	"encoding/binary"

	"github.com/segmentio/parquet-go/internal/bits"
	"golang.org/x/sys/cpu"
)

//go:noescape
func blockDeltaInt32(block *[blockSize]int32, lastValue int32) int32

//go:noescape
func blockDeltaInt32AVX2(block *[blockSize]int32, lastValue int32) int32

//go:noescape
func blockMinInt32(block *[blockSize]int32) int32

//go:noescape
func blockMinInt32AVX2(block *[blockSize]int32) int32

//go:noescape
func blockSubInt32(block *[blockSize]int32, value int32)

//go:noescape
func blockSubInt32AVX2(block *[blockSize]int32, value int32)

//go:noescape
func blockBitWidthsInt32(bitWidths *[numMiniBlocks]byte, block *[blockSize]int32)

//go:noescape
func blockBitWidthsInt32AVX2(bitWidths *[numMiniBlocks]byte, block *[blockSize]int32)

//go:noescape
func miniBlockCopyInt32x1bitAVX2(dst *byte, src *[miniBlockSize]int32)

//go:noescape
func miniBlockCopyInt32x32bitsAVX2(dst *byte, src *[miniBlockSize]int32)

func blockClearInt32(block *[blockSize]int32, blockLength int) {
	if blockLength < blockSize {
		clear := block[blockLength:]
		for i := range clear {
			clear[i] = 0
		}
	}
}

func (e *BinaryPackedEncoding) encodeInt32(dst []byte, src []int32) []byte {
	totalValues := len(src)
	firstValue := int32(0)
	if totalValues > 0 {
		firstValue = src[0]
	}

	n := len(dst)
	dst = resize(dst, n+maxHeaderLength)
	dst = dst[:n+encodeBinaryPackedHeader(dst[n:], blockSize, numMiniBlocks, totalValues, int64(firstValue))]

	if totalValues < 2 {
		return dst
	}

	lastValue := firstValue

	for i := 1; i < len(src); i += blockSize {
		block := [blockSize]int32{}
		blockLength := copy(block[:], src[i:])
		switch {
		case cpu.X86.HasAVX2:
			dst, lastValue = e.encodeInt32BlockAVX2(dst, &block, blockLength, lastValue)
		default:
			dst, lastValue = e.encodeInt32Block(dst, &block, blockLength, lastValue)
		}
	}

	return dst
}

func (e *BinaryPackedEncoding) encodeInt32Block(dst []byte, block *[blockSize]int32, blockLength int, lastValue int32) ([]byte, int32) {
	lastValue = blockDeltaInt32(block, lastValue)
	minDelta := blockMinInt32(block)
	blockSubInt32(block, minDelta)
	blockClearInt32(block, blockLength)

	bitWidths := [numMiniBlocks]byte{}
	blockBitWidthsInt32(&bitWidths, block)

	n := len(dst)
	dst = resize(dst, n+maxMiniBlockLength)
	n += encodeBlockHeader(dst[n:], int64(minDelta), bitWidths)

	for i, bitWidth := range bitWidths {
		if bitWidth != 0 {
			miniBlock := (*[miniBlockSize]int32)(block[i*miniBlockSize:])
			bitOffset := uint(n) * 8

			for _, bits := range miniBlock {
				for b := uint(0); b < uint(bitWidth); b++ {
					x := bitOffset / 8
					y := bitOffset % 8
					dst[x] |= byte(((bits >> b) & 1) << y)
					bitOffset++
				}
			}

			n += (miniBlockSize * int(bitWidth)) / 8
		}
	}

	return dst[:n], lastValue
}

func (e *BinaryPackedEncoding) encodeInt32BlockAVX2(dst []byte, block *[blockSize]int32, blockLength int, lastValue int32) ([]byte, int32) {
	lastValue = blockDeltaInt32AVX2(block, lastValue)
	minDelta := blockMinInt32AVX2(block)
	blockSubInt32AVX2(block, minDelta)
	blockClearInt32(block, blockLength)

	bitWidths := [numMiniBlocks]byte{}
	blockBitWidthsInt32AVX2(&bitWidths, block)

	n := len(dst)
	dst = resize(dst, n+maxMiniBlockLength)
	n += encodeBlockHeader(dst[n:], int64(minDelta), bitWidths)

	for i, bitWidth := range bitWidths {
		if bitWidth != 0 {
			out := &dst[n]
			miniBlock := (*[miniBlockSize]int32)(block[i*miniBlockSize:])

			switch bitWidth {
			case 1:
				miniBlockCopyInt32x1bitAVX2(out, miniBlock)
			case 32:
				miniBlockCopyInt32x32bitsAVX2(out, miniBlock)
			default:
				bitOffset := uint(n) * 8
				for _, bits := range miniBlock {
					for b := uint(0); b < uint(bitWidth); b++ {
						x := bitOffset / 8
						y := bitOffset % 8
						dst[x] |= byte(((bits >> b) & 1) << y)
						bitOffset++
					}
				}
			}

			n += (miniBlockSize * int(bitWidth)) / 8
		}
	}

	return dst[:n], lastValue
}

func (e *BinaryPackedEncoding) encodeInt64(dst []byte, src []int64) []byte {
	totalValues := len(src)
	firstValue := int64(0)
	if totalValues > 0 {
		firstValue = src[0]
	}
	dst = appendBinaryPackedHeader(dst, blockSize, numMiniBlocks, totalValues, firstValue)
	if totalValues < 2 {
		return dst
	}

	lastValue := firstValue
	for i := 1; i < totalValues; {
		block := make([]int64, blockSize)
		block = block[:copy(block, src[i:])]
		i += len(block)

		for j, v := range block {
			block[j], lastValue = v-lastValue, v
		}

		minDelta := bits.MinInt64(block)
		bits.SubInt64(block, minDelta)

		// blockSize x 8: we store at most `blockSize` count of values, which
		// might be up to 64 bits in length, which is why we multiple by 8.
		//
		// Technically we could size the buffer to a smaller size when the
		// bit width requires less than 8 bytes per value, but it would cause
		// the buffer to be put on the heap since the compiler wouldn't know
		// how much stack space it needs in advance.
		miniBlock := make([]byte, blockSize*8)
		bitWidths := [numMiniBlocks]byte{}
		bitOffset := uint(0)
		miniBlockLength := 0

		for i := range bitWidths {
			j := (i + 0) * miniBlockSize
			k := (i + 1) * miniBlockSize

			if k > len(block) {
				k = len(block)
			}

			bitWidth := uint(bits.MaxLen64(block[j:k]))
			if bitWidth != 0 {
				bitWidths[i] = byte(bitWidth)

				for _, bits := range block[j:k] {
					for b := uint(0); b < bitWidth; b++ {
						x := bitOffset / 8
						y := bitOffset % 8
						miniBlock[x] |= byte(((bits >> b) & 1) << y)
						bitOffset++
					}
				}

				miniBlockLength += (miniBlockSize * int(bitWidth)) / 8
			}

			if k == len(block) {
				break
			}
		}

		miniBlock = miniBlock[:miniBlockLength]
		dst = appendBinaryPackedBlock(dst, int64(minDelta), bitWidths)
		dst = append(dst, miniBlock...)
	}

	return dst
}

func encodeBinaryPackedHeader(dst []byte, blockSize, numMiniBlocks, totalValues int, firstValue int64) (n int) {
	n += binary.PutUvarint(dst[n:], uint64(blockSize))
	n += binary.PutUvarint(dst[n:], uint64(numMiniBlocks))
	n += binary.PutUvarint(dst[n:], uint64(totalValues))
	n += binary.PutVarint(dst[n:], firstValue)
	return n
}

func encodeBlockHeader(dst []byte, minDelta int64, bitWidths [numMiniBlocks]byte) (n int) {
	n += binary.PutVarint(dst, int64(minDelta))
	n += copy(dst[n:], bitWidths[:])
	return n
}

func appendBinaryPackedHeader(dst []byte, blockSize, numMiniBlocks, totalValues int, firstValue int64) []byte {
	b := [4 * binary.MaxVarintLen64]byte{}
	n := encodeBinaryPackedHeader(b[:], blockSize, numMiniBlocks, totalValues, firstValue)
	return append(dst, b[:n]...)
}

func appendBinaryPackedBlock(dst []byte, minDelta int64, bitWidths [numMiniBlocks]byte) []byte {
	b := [binary.MaxVarintLen64 + numMiniBlocks]byte{}
	n := encodeBlockHeader(b[:], minDelta, bitWidths)
	return append(dst, b[:n]...)
}

func resize(buf []byte, size int) []byte {
	if cap(buf) < size {
		newCap := 2 * cap(buf)
		if newCap < size {
			newCap = size
		}
		buf = append(make([]byte, 0, newCap), buf...)
	} else if size > len(buf) {
		clear := buf[len(buf):size]
		for i := range clear {
			clear[i] = 0
		}
	}
	return buf[:size]
}
