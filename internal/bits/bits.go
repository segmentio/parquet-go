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

func MinLeadingZeros32(data []int32) int {
	if len(data) == 0 {
		return 0
	}
	min := 32
	for _, v := range data {
		if n := bits.LeadingZeros32(uint32(v)); n < min {
			min = n
		}
	}
	return min
}

func MinLeadingZeros64(data []int64) int {
	if len(data) == 0 {
		return 0
	}
	min := 64
	for _, v := range data {
		if n := bits.LeadingZeros64(uint64(v)); n < min {
			min = n
		}
	}
	return min
}

func MinLeadingZeros96(data [][12]byte) int {
	if len(data) == 0 {
		return 0
	}
	min := 96
	for i := range data {
		p := unsafe.Pointer(&data[i][0])
		// assume little endian
		hi := *(*uint64)(unsafe.Add(p, 4))
		lo := *(*uint32)(p)
		switch {
		case hi != 0:
			if n := bits.LeadingZeros64(hi); n < min {
				min = n
			}
		case lo != 0:
			if n := 64 + bits.LeadingZeros32(lo); n < min {
				min = n
			}
		}
	}
	return min
}
