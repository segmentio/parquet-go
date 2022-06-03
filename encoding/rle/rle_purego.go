//go:build purego || !amd64

package rle

import "encoding/binary"

func isZero(data []byte) bool {
	for i := range data {
		if data[i] != 0 {
			return false
		}
	}
	return true
}

func isOnes(data []byte) bool {
	for i := range data {
		if data[i] != 0xFF {
			return false
		}
	}
	return true
}

func encodeBytesBitpack(dst []byte, src []uint64, bitWidth uint) int {
	bitMask := uint64(1<<bitWidth) - 1
	offset := 0

	for _, word := range src {
		word = (word & bitMask) |
			(((word >> 8) & bitMask) << (1 * bitWidth)) |
			(((word >> 16) & bitMask) << (2 * bitWidth)) |
			(((word >> 24) & bitMask) << (3 * bitWidth)) |
			(((word >> 32) & bitMask) << (4 * bitWidth)) |
			(((word >> 40) & bitMask) << (5 * bitWidth)) |
			(((word >> 48) & bitMask) << (6 * bitWidth)) |
			(((word >> 56) & bitMask) << (7 * bitWidth))
		binary.LittleEndian.PutUint64(dst[offset:], word)
		offset += int(bitWidth)
	}

	return offset
}
