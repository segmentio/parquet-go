package bits

import (
	"math/bits"
	"unsafe"
)

func BitCount(count int) uint {
	return 8 * uint(count)
}

func ByteCount(count uint) int {
	return int((count + 7) / 8)
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

func MaxLen32(data []int32) int {
	max := 0
	for _, v := range data {
		if n := bits.Len32(uint32(v)); n > max {
			max = n
		}
	}
	return max
}

func MaxLen64(data []int64) int {
	max := 0
	for _, v := range data {
		if n := bits.Len64(uint64(v)); n > max {
			max = n
		}
	}
	return max
}

func MaxLen96(data [][12]byte) int {
	max := 0
	for i := range data {
		p := unsafe.Pointer(&data[i][0])
		// assume little endian
		hi := *(*uint64)(unsafe.Add(p, 4))
		lo := *(*uint32)(p)
		switch {
		case hi != 0:
			if n := bits.Len64(hi) + 32; n > max {
				max = n
			}
		case lo != 0:
			if n := bits.Len32(lo); n > max {
				max = n
			}
		}
	}
	return max
}

func MinLeadingZeros32(data []int32) int {
	if len(data) == 0 {
		return 0
	}
	return 32 - MaxLen32(data)
}

func MinLeadingZeros64(data []int64) int {
	if len(data) == 0 {
		return 0
	}
	return 64 - MaxLen64(data)
}

func MinLeadingZeros96(data [][12]byte) int {
	if len(data) == 0 {
		return 0
	}
	return 96 - MaxLen96(data)
}
