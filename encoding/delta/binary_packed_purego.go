//go:build purego || !amd64

package delta

import (
	"encoding/binary"

	"github.com/segmentio/parquet-go/internal/bits"
)

func (e *BinaryPackedEncoding) encodeInt32(dst []byte, src []int32) []byte {
	totalValues := len(src)
	firstValue := int32(0)
	if totalValues > 0 {
		firstValue = src[0]
	}
	dst = appendBinaryPackedHeader(dst, blockSize, numMiniBlocks, totalValues, int64(firstValue))
	if totalValues < 2 {
		return dst
	}

	lastValue := firstValue
	for i := 1; i < len(src); {
		block := make([]int32, blockSize)
		block = block[:copy(block, src[i:])]
		i += len(block)

		for j, v := range block {
			block[j], lastValue = v-lastValue, v
		}

		minDelta := bits.MinInt32(block)
		bits.SubInt32(block, minDelta)

		miniBlock := make([]byte, blockSize*4)
		bitWidths := make([]byte, numMiniBlocks)
		bitOffset := uint(0)
		miniBlockLength := 0

		for i := range bitWidths {
			j := (i + 0) * miniBlockSize
			k := (i + 1) * miniBlockSize

			if k > len(block) {
				k = len(block)
			}

			bitWidth := uint(bits.MaxLen32(block[j:k]))
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

	return dst
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
