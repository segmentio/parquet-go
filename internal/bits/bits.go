package bits

import (
	"encoding/binary"
	"math/bits"
)

func BitCount(count int) uint {
	return 8 * uint(count)
}

func ByteCount(count uint) int {
	return int((count + 7) / 8)
}

func Round(count uint) uint {
	return BitCount(ByteCount(count))
}

func Len32(i int32) int {
	if i < 0 {
		i = -i
	}
	return bits.Len32(uint32(i))
}

func Len64(i int64) int {
	if i < 0 {
		i = -i
	}
	return bits.Len64(uint64(i))
}

func IndexShift8(bitIndex uint) (index, shift uint) {
	return bitIndex / 8, bitIndex % 8
}

func IndexShift16(bitIndex uint) (index, shift uint) {
	return bitIndex / 16, bitIndex % 16
}

func IndexShift32(bitIndex uint) (index, shift uint) {
	return bitIndex / 32, bitIndex % 32
}

func IndexShift64(bitIndex uint) (index, shift uint) {
	return bitIndex / 64, bitIndex % 64
}

func CompareInt96(v1, v2 [12]byte) int {
	hi1 := binary.LittleEndian.Uint32(v1[8:])
	hi2 := binary.LittleEndian.Uint32(v2[8:])

	switch {
	case hi1 < hi2:
		return -1
	case hi1 > hi2:
		return +1
	}

	lo1 := binary.LittleEndian.Uint64(v1[:8])
	lo2 := binary.LittleEndian.Uint64(v2[:8])

	switch {
	case lo1 < lo2:
		return -1
	case lo1 > lo2:
		return +1
	default:
		return 0
	}
}
