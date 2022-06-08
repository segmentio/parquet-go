package bits

import (
	"math/bits"
)

type uint128 = [16]byte

func BitCount(count int) uint {
	return 8 * uint(count)
}

func ByteCount(count uint) int {
	return int((count + 7) / 8)
}

func Len8(i int8) int {
	return bits.Len8(uint8(i))
}

func Len16(i int16) int {
	return bits.Len16(uint16(i))
}

func Len32(i int32) int {
	return bits.Len32(uint32(i))
}

func Len64(i int64) int {
	return bits.Len64(uint64(i))
}
