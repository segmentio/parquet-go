//go:build purego || !amd64

package delta

import (
	"encoding/binary"

	"github.com/segmentio/parquet-go/internal/bits"
)

func miniBlockPackInt32(dst []byte, src *[miniBlockSize]int32, bitWidth uint) {
	bitMask := uint32(1<<bitWidth) - 1
	bitOffset := uint(0)

	for _, value := range src {
		i := bitOffset / 32
		j := bitOffset % 32

		lo := binary.LittleEndian.Uint32(dst[(i+0)*4:])
		hi := binary.LittleEndian.Uint32(dst[(i+1)*4:])

		lo |= (uint32(value) & bitMask) << j
		hi |= (uint32(value) >> (32 - j))

		binary.LittleEndian.PutUint32(dst[(i+0)*4:], lo)
		binary.LittleEndian.PutUint32(dst[(i+1)*4:], hi)

		bitOffset += bitWidth
	}
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
