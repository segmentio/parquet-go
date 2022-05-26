//go:build !purego

package delta

import (
	"encoding/binary"

	"github.com/segmentio/parquet-go/internal/bits"
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
func miniBlockBitWidthsInt32(bitWidths *[numMiniBlocks]byte, block *[blockSize]int32)

func encodeInt32(dst []byte, src []int32) (n int) {
	for i := range dst {
		dst[i] = 0
	}

	totalValues := len(src)
	firstValue := int32(0)
	if totalValues > 0 {
		firstValue = src[0]
	}
	n += encodeBinaryPackedHeader(dst, blockSize, numMiniBlocks, totalValues, int64(firstValue))
	if totalValues < 2 {
		return n
	}

	lastValue := firstValue

	for i := 1; i < len(src); {
		var block *[blockSize]int32
		var blockLength int
		if remain := src[i:]; len(remain) >= blockSize {
			block = (*[blockSize]int32)(remain)
			blockLength = blockSize
		} else {
			blockBuffer := [blockSize]int32{}
			blockLength = copy(blockBuffer[:], remain)
			block = &blockBuffer
		}
		i += blockLength

		lastValue = blockDeltaInt32(block, lastValue)
		minDelta := blockMinInt32(block)
		blockSubInt32(block, minDelta)

		n += binary.PutVarint(dst[n:], int64(minDelta))
		n += numMiniBlocks

		bitWidths := dst[n-numMiniBlocks : n : n]
		miniBlockBitWidthsInt32((*[numMiniBlocks]byte)(bitWidths), block)

		for i, bitWidth := range bitWidths {
			j := (i + 0) * miniBlockSize
			k := (i + 1) * miniBlockSize

			if k > blockLength {
				k = blockLength
			}

			if bitWidth != 0 {
				miniBlockLength := (miniBlockSize * int(bitWidth)) / 8
				n += miniBlockLength

				miniBlock := dst[n-miniBlockLength : n : n]
				bitOffset := uint(0)

				for _, bits := range block[j:k] {
					for b := uint(0); b < uint(bitWidth); b++ {
						x := bitOffset / 8
						y := bitOffset % 8
						miniBlock[x] |= byte(((bits >> b) & 1) << y)
						bitOffset++
					}
				}
			}

			if k == blockLength {
				break
			}
		}
	}

	return n
}

func encodeInt64(dst []byte, src []int64) (n int) {
	totalValues := len(src)
	firstValue := int64(0)
	if totalValues > 0 {
		firstValue = src[0]
	}
	dst = dst[:0]
	dst = appendBinaryPackedHeader(dst, blockSize, numMiniBlocks, totalValues, firstValue)
	if totalValues < 2 {
		return len(dst)
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
		bitWidths := make([]byte, numMiniBlocks)
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

	return len(dst)
}

func appendBinaryPackedHeader(dst []byte, blockSize, numMiniBlocks, totalValues int, firstValue int64) []byte {
	b := [4 * binary.MaxVarintLen64]byte{}
	n := 0
	n += binary.PutUvarint(b[n:], uint64(blockSize))
	n += binary.PutUvarint(b[n:], uint64(numMiniBlocks))
	n += binary.PutUvarint(b[n:], uint64(totalValues))
	n += binary.PutVarint(b[n:], firstValue)
	return append(dst, b[:n]...)
}

func appendBinaryPackedBlock(dst []byte, minDelta int64, bitWidths []byte) []byte {
	b := [binary.MaxVarintLen64]byte{}
	n := binary.PutVarint(b[:], minDelta)
	dst = append(dst, b[:n]...)
	dst = append(dst, bitWidths...)
	return dst
}

func encodeBinaryPackedHeader(dst []byte, blockSize, numMiniBlocks, totalValues int, firstValue int64) (n int) {
	n += binary.PutUvarint(dst[n:], uint64(blockSize))
	n += binary.PutUvarint(dst[n:], uint64(numMiniBlocks))
	n += binary.PutUvarint(dst[n:], uint64(totalValues))
	n += binary.PutVarint(dst[n:], firstValue)
	return n
}

func encodeBinaryPackedBlock(dst []byte, minDelta int64, bitWidths [numMiniBlocks]byte) int {
	n := binary.PutVarint(dst, minDelta)
	n += copy(dst[n:], bitWidths[:])
	return n
}
